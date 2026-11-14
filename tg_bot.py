"""
Публичный Telegram-бот: любой пишет /start и получает алерты о прострелах.

Список подписчиков хранится в самом Telegram — JSON-файлом, закреплённым в
личном чате админа с ботом (админ = первый chat id в TG_CHAT_ID). Так он
переживает перезапуски Render, где нет постоянного диска, и не нужна база.

Команды: /start, /stop, /porog N, /status, /help.
"""
from __future__ import annotations

import io
import json
import threading
import time

import requests

HELP = (
    "⚡ <b>Скринер прострелов MOEX FORTS</b>\n"
    "Присылаю принты по фьючам Мосбиржи, которые улетели на ±{thr}% и больше за секунду.\n\n"
    "/start — подписаться\n"
    "/stop — отписаться\n"
    "/porog 3 — присылать только прострелы от 3% (не меньше {thr}%)\n"
    "/status — мои настройки"
)


class BotHub:
    def __init__(self, token: str, admin_chat: str, min_thr: float, log, status_fn=None):
        self.token = token
        self.admin = admin_chat
        self.min_thr = min_thr
        self.log = log
        self.status_fn = status_fn or (lambda: "")
        self.api = f"https://api.telegram.org/bot{token}"
        self.s = requests.Session()
        self.subs: dict[str, dict] = {}          # chat_id -> {"thr": float, "name": str}
        self.lock = threading.Lock()
        self.dirty = False
        self.pinned_id: int | None = None
        self.offset = 0

    # ───────── Telegram API ─────────
    def call(self, method: str, timeout: float = 15, files=None, **params):
        r = self.s.post(f"{self.api}/{method}", data=params, files=files, timeout=timeout)
        try:
            js = r.json()
        except ValueError:
            js = {"ok": False, "description": r.text[:200], "error_code": r.status_code}
        return js

    def send(self, chat: str, text: str) -> str:
        """'ok' | 'gone' (пользователь заблокировал бота) | 'error'."""
        for _ in range(3):
            try:
                js = self.call("sendMessage", chat_id=chat, text=text, parse_mode="HTML",
                               disable_web_page_preview=True)
            except requests.RequestException:
                time.sleep(2)
                continue
            if js.get("ok"):
                return "ok"
            code = js.get("error_code")
            if code == 429:
                time.sleep(min(float((js.get("parameters") or {}).get("retry_after", 3)), 30))
                continue
            if code == 403 or (code == 400 and "chat not found" in str(js.get("description"))):
                return "gone"
            self.log(f"[bot] {chat}: {code} {js.get('description')}")
            return "error"
        return "error"

    # ───────── хранение подписчиков ─────────
    def load(self):
        try:
            js = self.call("getChat", chat_id=self.admin)
            pm = (js.get("result") or {}).get("pinned_message") or {}
            doc = pm.get("document") or {}
            if not doc.get("file_name", "").startswith("subscribers"):
                self.log("[bot] сохранённых подписчиков нет — начинаю с пустого списка")
                return
            f = self.call("getFile", file_id=doc["file_id"]).get("result") or {}
            raw = self.s.get(f"https://api.telegram.org/file/bot{self.token}/{f['file_path']}",
                             timeout=20).content
            data = json.loads(raw.decode("utf-8"))
            with self.lock:
                self.subs = {str(k): v for k, v in data.get("subs", {}).items()}
            self.pinned_id = pm.get("message_id")
            self.log(f"[bot] подписчиков загружено: {len(self.subs)}")
        except Exception as e:
            self.log(f"[bot] не смог прочитать список подписчиков: {type(e).__name__}: {e}")

    def save(self):
        with self.lock:
            if not self.dirty:
                return
            self.dirty = False
            payload = json.dumps({"subs": self.subs, "saved": int(time.time())},
                                 ensure_ascii=False).encode("utf-8")
            n = len(self.subs)
        try:
            js = self.call("sendDocument", timeout=30, chat_id=self.admin,
                           disable_notification=True,
                           caption=f"🗂 Список подписчиков бота ({n}). Не открепляй это сообщение.",
                           files={"document": ("subscribers.json", io.BytesIO(payload),
                                               "application/json")})
            if not js.get("ok"):
                raise RuntimeError(js.get("description"))
            mid = js["result"]["message_id"]
            self.call("pinChatMessage", chat_id=self.admin, message_id=mid,
                      disable_notification=True)
            if self.pinned_id and self.pinned_id != mid:
                self.call("deleteMessage", chat_id=self.admin, message_id=self.pinned_id)
            self.pinned_id = mid
        except Exception as e:
            self.log(f"[bot] не смог сохранить подписчиков: {e}")
            with self.lock:
                self.dirty = True

    # ───────── команды ─────────
    def handle(self, msg: dict):
        chat = msg.get("chat") or {}
        cid = str(chat.get("id", ""))
        text = (msg.get("text") or "").strip()
        if not cid or not text.startswith("/"):
            return
        cmd, _, arg = text.partition(" ")
        cmd = cmd.split("@")[0].lower()
        name = chat.get("username") or chat.get("first_name") or chat.get("title") or ""
        thr0 = f"{self.min_thr:g}"
        if cmd == "/start":
            with self.lock:
                new = cid not in self.subs
                self.subs.setdefault(cid, {"name": name})   # порог — общий, пока не задан /porog
                self.dirty |= new
            self.send(cid, ("✅ Подписка оформлена.\n\n" if new else "Ты уже подписан(а).\n\n")
                      + HELP.format(thr=thr0))
            if new:
                self.log(f"[bot] новый подписчик: {name} ({cid}), всего {len(self.subs)}")
        elif cmd == "/stop":
            with self.lock:
                gone = self.subs.pop(cid, None) is not None
                self.dirty |= gone
            self.send(cid, "🔕 Отписка. Вернуться — /start" if gone else "Ты и так не подписан(а). /start")
        elif cmd == "/porog":
            try:
                v = float(arg.replace(",", ".").replace("%", ""))
            except ValueError:
                self.send(cid, f"Напиши число, например: /porog 3 (минимум {thr0}%)")
                return
            v = max(v, self.min_thr)
            with self.lock:
                sub = self.subs.setdefault(cid, {"name": name})
                sub["thr"], sub["custom"] = v, True
                self.dirty = True
            self.send(cid, f"Ок, присылаю прострелы от ±{v:g}%")
        elif cmd == "/status":
            with self.lock:
                me = self.subs.get(cid)
            mine = (f"подписка: ✅, порог ±{self.user_thr(me):g}%"
                    + ("" if me.get("custom") else " (общий)")) if me else "подписка: ❌ (/start)"
            self.send(cid, f"{mine}\n{self.status_fn()}")
        else:
            self.send(cid, HELP.format(thr=thr0))

    def poll_forever(self):
        self.call("deleteWebhook")
        self.call("setMyCommands", commands=json.dumps([
            {"command": "start", "description": "подписаться на прострелы"},
            {"command": "stop", "description": "отписаться"},
            {"command": "porog", "description": "свой порог, например /porog 3"},
            {"command": "status", "description": "мои настройки"},
        ]))
        last_save = time.time()
        while True:
            try:
                # long polling: Telegram держит запрос до 50 с, пока не придут сообщения
                js = self.s.post(f"{self.api}/getUpdates", timeout=65, data={
                    "offset": self.offset, "timeout": 50,
                    "allowed_updates": json.dumps(["message"])}).json()
                if not js.get("ok"):
                    # 409 — при деплое ненадолго работают две копии; ждём, пока старая умрёт
                    time.sleep(5)
                    continue
                for upd in js.get("result", []):
                    self.offset = upd["update_id"] + 1
                    if upd.get("message"):
                        try:
                            self.handle(upd["message"])
                        except Exception as e:
                            self.log(f"[bot] ошибка обработки команды: {e}")
            except Exception:
                time.sleep(3)
            if time.time() - last_save > 5:
                last_save = time.time()
                self.save()

    def start(self):
        self.load()
        threading.Thread(target=self.poll_forever, daemon=True, name="tg-bot").start()

    def user_thr(self, sub: dict) -> float:
        """Свой порог — только если задан через /porog; иначе общий THRESHOLD."""
        if sub.get("custom"):
            return max(float(sub.get("thr", self.min_thr)), self.min_thr)
        return self.min_thr

    # ───────── рассылка ─────────
    def broadcast(self, text: str, pct: float, skip: set[str] = frozenset()):
        with self.lock:
            targets = [(c, self.user_thr(v)) for c, v in self.subs.items()]
        gone = []
        for chat, thr in targets:
            if chat in skip or abs(pct) < thr:
                continue
            if self.send(chat, text) == "gone":
                gone.append(chat)
            time.sleep(0.04)  # общий лимит Telegram ~30 сообщений/с
        if gone:
            with self.lock:
                for c in gone:
                    self.subs.pop(c, None)
                self.dirty = True
            self.log(f"[bot] удалены заблокировавшие бота: {len(gone)}")
