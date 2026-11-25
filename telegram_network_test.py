"""Быстрая проверка доступа контейнера к Telegram API без вывода токена."""
from __future__ import annotations

import json
import os
import socket
import sys
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def main() -> int:
    print("1) DNS api.telegram.org")
    try:
        addresses = sorted({item[4][0] for item in socket.getaddrinfo("api.telegram.org", 443)})
        print("   OK:", ", ".join(addresses))
    except OSError as exc:
        print("   ERROR:", exc)
        return 2

    proxy = str(os.getenv("TELEGRAM_PROXY_URL") or "").strip()
    handlers = []
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        print("2) Используется TELEGRAM_PROXY_URL")
    else:
        print("2) Прокси не задан")
    opener = urllib.request.build_opener(*handlers)

    print("3) HTTPS до api.telegram.org")
    try:
        with opener.open("https://api.telegram.org", timeout=30) as response:
            print("   OK, HTTP", response.status)
    except urllib.error.HTTPError as exc:
        # 404/302 тоже означают, что соединение с сервером установлено.
        print("   OK, Telegram ответил HTTP", exc.code)
    except Exception as exc:
        print("   ERROR:", type(exc).__name__, exc)
        return 3

    token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        print("4) TELEGRAM_BOT_TOKEN не найден — getMe пропущен")
        return 0

    print("4) Проверка токена через getMe")
    url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        with opener.open(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if payload.get("ok"):
            result = payload.get("result") or {}
            print("   OK: @" + str(result.get("username") or "без_username"))
            return 0
        print("   ERROR: Telegram отклонил токен:", payload.get("description"))
        return 4
    except urllib.error.HTTPError as exc:
        print("   ERROR: HTTP", exc.code, "(токен может быть неверным или отозванным)")
        return 4
    except Exception as exc:
        print("   ERROR:", type(exc).__name__, exc)
        return 5


if __name__ == "__main__":
    sys.exit(main())
