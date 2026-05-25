FROM python:3.11-slim

# 設定環境變數
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 安裝基本系統依賴（Telethon 可能需要一些編譯工具，slim 版建議加上 git/gcc 以防萬一）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 複製依賴並安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 創建 data 目錄
RUN mkdir -p /app/data

# 複製程式碼
COPY main.py .

# 這是解決 Oracle 主機權限的核心：
# 在容器內創建一個 UID 1000 的 appuser，與主機的 opc/ubuntu 用戶對齊
RUN useradd -u 1001 -m appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]