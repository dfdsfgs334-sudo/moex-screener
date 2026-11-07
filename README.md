# MOEX FORTS — скринер прострелов

Следит за всеми фьючерсами срочного рынка Мосбиржи и шлёт алерт на каждое движение от 2% в любую сторону.

## Типы сигналов
- **Прострел (IMPULSE)**: цена ушла на ±2% от минимума или максимума за последние N минут (по умолчанию 5). Включён всегда.
- **К клирингу (DAY)**: изменение к вчерашнему клирингу пробило ±2%, затем ±3%, ±4% и так далее. Это не прострел, а набранное за день движение, поэтому **по умолчанию выключено**. Включается флагом `--day` или переменной `DAY_ALERTS=1`.

Защита от спама: повторный алерт по тому же тикеру в ту же сторону приходит не раньше чем через 15 минут. Исключение: движение выросло ещё на 1% или больше.

## Запуск
```bash
pip install -r requirements.txt
python moex_spike_screener.py
```

## Telegram
1. Создай бота через @BotFather и возьми токен.
2. Напиши боту что-нибудь, затем узнай свой chat_id: открой `https://api.telegram.org/bot<TOKEN>/getUpdates`.
```bash
export TG_TOKEN=123456:ABC...
export TG_CHAT_ID=123456789
python moex_spike_screener.py --test-alert
```

## Полезные флаги
| флаг | что делает |
|---|---|
| `-t 2` | порог в % |
| `-w 5` | окно импульса в минутах (`-w 1` ловит совсем резкие свечи) |
| `-i 5` | период опроса в секундах |
| `--front` | только ближайший контракт по каждому активу |
| `--min-trades 30` | отсекает неликвид, где одна сделка даёт фейковый прострел |
| `--only Si,BR,MX,GD` | только эти активы или тикеры |
| `--exclude ...` | исключить активы или тикеры |
| `--day` | включить дневные алерты к клирингу (или `DAY_ALERTS=1` в Environment) |
| `--plain` | без дашборда, вывод строками (для сервера, nohup, systemd) |

## Задержка данных
Анонимный ISS отдаёт котировки FORTS с задержкой около 15 минут. Для данных в реальном времени нужна подписка на ISS. С ней задай переменные окружения, и скрипт залогинится через MOEX Passport:
```bash
export MOEX_LOGIN=...
export MOEX_PASSWORD=...
```

---

## Деплой на бесплатный хостинг (Render + пингер)

Сервис поднимает веб-дашборд на `/` и отдаёт `ok` на `/health`. Алерты уходят в Telegram.

### 1. Залей код на GitHub
Создай репозиторий (можно приватный) и запушь эту папку:
```bash
git init && git add . && git commit -m "moex screener" && git branch -M main
```
```bash
git remote add origin https://github.com/<ты>/moex-screener.git && git push -u origin main
```

### 2. Render
1. Зарегистрируйся на https://render.com через GitHub.
2. Нажми **New → Blueprint**, выбери репозиторий. Render прочитает `render.yaml` и создаст free web service из `Dockerfile`.
3. Впиши `TG_TOKEN` и `TG_CHAT_ID`, когда Render их запросит. Остальные переменные (`THRESHOLD`, `WINDOW_MIN`, `MIN_TRADES`) меняются в разделе Environment.
4. Через пару минут сервис будет доступен по адресу вида `https://moex-spike-screener.onrender.com`.

### 3. Не дай ему уснуть
Free-сервис Render засыпает через 15 минут без входящих запросов. Пока он спит, скринер не работает.
1. Зарегистрируйся на https://cron-job.org (бесплатно).
2. Создай задачу: URL `https://<твой-сервис>.onrender.com/health`, интервал каждые 5 минут.

Free-план Render даёт 750 часов в месяц, этого хватает на один сервис круглосуточно. При рестарте сервиса теряется только история окна за последние минуты.

### Альтернатива надёжнее: Oracle Cloud Always Free VM
Бесплатная виртуалка навсегда. Нужна карта для верификации, деньги не списываются. Скринер на ней не засыпает и не нуждается в пингере:
```bash
sudo apt update && sudo apt install -y python3-pip git && git clone https://github.com/<ты>/moex-screener.git && cd moex-screener && pip3 install -r requirements.txt
```
Затем создай `/etc/systemd/system/moex.service`:
```ini
[Unit]
Description=MOEX spike screener
After=network-online.target

[Service]
WorkingDirectory=/home/ubuntu/moex-screener
Environment=TG_TOKEN=... TG_CHAT_ID=... TZ=Europe/Moscow PORT=8080
ExecStart=/usr/bin/python3 moex_spike_screener.py --plain --no-beep
Restart=always

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl enable --now moex && journalctl -u moex -f
```
