FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 TZ=Europe/Moscow PORT=8080
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY moex_spike_screener.py .
EXPOSE 8080
CMD ["python", "moex_spike_screener.py", "--plain", "--no-beep", "--csv", ""]
