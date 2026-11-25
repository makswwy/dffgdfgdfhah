from __future__ import annotations

import threading

# Общие блокировки используются VK- и Telegram-потоками в одном процессе.
# Это не даёт двум платформам одновременно перезаписать один JSON-файл.
APPLICATIONS_LOCK = threading.RLock()
PM_JOBS_LOCK = threading.RLock()
TELEGRAM_USERS_LOCK = threading.RLock()

# Очередь синхронных рассылок VK -> Telegram.
TELEGRAM_BROADCAST_LOCK = threading.RLock()
