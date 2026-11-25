from __future__ import annotations

import json
import os
import random
import re
import threading
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.parse import quote, urljoin
from html.parser import HTMLParser
import html
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import vk_api
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.upload import VkUpload

from config import COMMUNITY_MESSAGES_LINK, MESSAGE_CONFIG, admin_ids, cd_min, dev_ids, group_id, group_token, interval_sec
from handlers.chat_handler import build_order_action_keyboard, build_orders_history_keyboard, build_orders_list_keyboard, parse_message_payload, render_order_details, render_order_result, render_orders_history_text, render_orders_list_text
from menu import generate_main_menu
from services.ad_safety import detect_military_recruitment, render_military_recruitment_block_notice
from services.media_store import normalize_photo_files, save_photo_files_from_attachments, save_photo_payload, split_attachment_refs
from services.order_service import OrderService
from utils.db import ensure_json_file, read_json, write_json_atomic
from utils.logger import get_logger
from shared_state import APPLICATIONS_LOCK, PM_JOBS_LOCK


logger = get_logger()


class ResilientVkBotLongPoll:
    """VK Long Poll, который сам переподключается и не завершает весь бот."""

    def __init__(self, session: Any, community_id: int) -> None:
        self.session = session
        self.community_id = int(community_id)

    def listen(self):
        global longpoll_enabled

        retry_delay = 1.0
        while True:
            try:
                current = VkBotLongPoll(self.session, self.community_id)
                longpoll_enabled = True
                logger.info("VK Long Poll подключен")

                for event in current.listen():
                    retry_delay = 1.0
                    yield event

                # Нормально listen() практически не должен завершаться.
                # Если завершился — считаем это разрывом и подключаемся снова.
                longpoll_enabled = False
                logger.warning("VK Long Poll завершил listen(); переподключение...")

            except KeyboardInterrupt:
                raise
            except Exception:
                longpoll_enabled = False
                logger.exception(
                    "VK Long Poll оборвался. Бот НЕ перезапускается; "
                    "переподключение через %.1f сек.",
                    retry_delay,
                )

            time.sleep(retry_delay)
            retry_delay = min(30.0, retry_delay * 2.0)

# Можно использовать БЕЗ .env и БЕЗ дополнительных файлов:
# вставьте свой user token между кавычками в этой строке.
INLINE_VK_USER_TOKEN = ""

OWNER_ID = 574393629
SECOND_OWNER_ID = 1094157988
OWNER_IDS = {OWNER_ID, SECOND_OWNER_ID}

# Оба аккаунта владельца всегда имеют роль разработчика,
# независимо от того, что записано в config.py/users_db.json.
dev_ids = list(dict.fromkeys([*list(dev_ids), *OWNER_IDS]))
BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data.json"
USERS_DB_PATH = BASE_DIR / "users_db.json"
INCOMES_PATH = BASE_DIR / "incomes.json"
INVITES_LOG_PATH = BASE_DIR / "invites_log.json"
APPLICATIONS_PATH = BASE_DIR / "applications.json"
PM_JOBS_PATH = BASE_DIR / "pm_jobs.json"
COMPANIES_PATH = BASE_DIR / "companies.json"
CHAT_SEARCH_STATE_PATH = BASE_DIR / "chat_search_seen.json"
FOUND_CHATS_DB_PATH = BASE_DIR / "found_chats_database.json"
RF_GEO_CACHE_PATH = BASE_DIR / "rf_search_locations.json"
BROADCAST_STATS_PATH = BASE_DIR / "broadcast_stats.json"
BUTTON_STATS_PATH = BASE_DIR / "button_click_stats.json"
APPLICATION_CAPTURE_STATE_PATH = BASE_DIR / "application_capture_state.json"
AUTOJOIN_STATE_PATH = BASE_DIR / "autojoin_state.json"
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK)


def now_msk_str() -> str:
    return now_msk().strftime("%Y-%m-%d %H:%M:%S")


def is_broadcast_quiet_time(moment: datetime | None = None) -> bool:
    """Запрещает рассылку ежедневно с 23:00 до 07:00 по Москве."""
    current = moment.astimezone(MSK) if moment is not None else now_msk()
    return current.hour >= BROADCAST_QUIET_START_HOUR or current.hour < BROADCAST_QUIET_END_HOUR


def next_broadcast_allowed_at(moment: datetime | None = None) -> datetime:
    current = moment.astimezone(MSK) if moment is not None else now_msk()
    if current.hour >= BROADCAST_QUIET_START_HOUR:
        return (current + timedelta(days=1)).replace(
            hour=BROADCAST_QUIET_END_HOUR, minute=0, second=0, microsecond=0
        )
    if current.hour < BROADCAST_QUIET_END_HOUR:
        return current.replace(
            hour=BROADCAST_QUIET_END_HOUR, minute=0, second=0, microsecond=0
        )
    return current

ensure_json_file(DATA_PATH, MESSAGE_CONFIG)
ensure_json_file(USERS_DB_PATH, {})
ensure_json_file(INCOMES_PATH, [])
ensure_json_file(INVITES_LOG_PATH, [])
ensure_json_file(APPLICATIONS_PATH, {"next_id": 1, "items": []})
ensure_json_file(PM_JOBS_PATH, {"next_id": 1, "items": []})
ensure_json_file(COMPANIES_PATH, {"next_id": 1, "items": []})
ensure_json_file(CHAT_SEARCH_STATE_PATH, {"seen": {}, "query_cursor": 0, "runs": 0})
ensure_json_file(FOUND_CHATS_DB_PATH, {"items": {}})
ensure_json_file(BROADCAST_STATS_PATH, {"version": 1, "runs": []})
ensure_json_file(BUTTON_STATS_PATH, {"version": 1, "events": [], "links": {}})
ensure_json_file(APPLICATION_CAPTURE_STATE_PATH, {"last_cmid": {}})
ensure_json_file(AUTOJOIN_STATE_PATH, {"processed": {}, "joined": 0, "skipped": 0, "errors": 0})

order_service = OrderService(BASE_DIR)
runtime_data = read_json(DATA_PATH, MESSAGE_CONFIG)

_broadcast_stats_lock = threading.RLock()
_button_stats_lock = threading.RLock()
broadcast_stats_thread: threading.Thread | None = None
BROADCAST_STATS_REFRESH_SECONDS = 120
BROADCAST_STATS_LIVE_HOURS = 36
BROADCAST_STATS_KEEP_DAYS = 90


# Состояния диалогов для административного редактирования заказов.
# Личные сообщения отключены, но эти состояния всё ещё используются в беседах.
USER_STATES: dict[str, dict[str, Any]] = {}


def _repair_mojibake(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        repaired = value.encode("cp1251").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value
    suspicious = ("Р", "С", "Ѓ", "Ћ", "Ђ")
    original_score = sum(value.count(ch) for ch in suspicious)
    repaired_score = sum(repaired.count(ch) for ch in suspicious)
    return repaired if repaired_score < original_score else value


chat_ids = runtime_data.get("chat_ids", MESSAGE_CONFIG.get("chat_ids", []))
admin_chat = runtime_data.get("admin_chat", MESSAGE_CONFIG.get("admin_chat"))
message_text = _repair_mojibake(runtime_data.get("message_text", runtime_data.get("text", MESSAGE_CONFIG.get("text", ""))))
message_photo_path = runtime_data.get("photo_path", MESSAGE_CONFIG.get("photo_path"))
message_photo_attachment = runtime_data.get("photo_attachment")
main_button_title = str(runtime_data.get("main_button_title") or "Работать с нами!").strip() or "Работать с нами!"
main_button_link = str(runtime_data.get("main_button_link") or COMMUNITY_MESSAGES_LINK).strip() or COMMUNITY_MESSAGES_LINK

vk_session = vk_api.VkApi(token=group_token)
vk = vk_session.get_api()
longpoll = ResilientVkBotLongPoll(vk_session, group_id)
longpoll_enabled = True
_main_vk_thread_ident = threading.get_ident()
# Загрузка медиа вынесена в отдельную HTTP-сессию: тяжёлый upload больше не
# конкурирует с Long Poll, callback-ответами и обычными командами.
media_vk_session = vk_api.VkApi(token=group_token)
media_vk = media_vk_session.get_api()
media_vk_upload = VkUpload(media_vk_session)

ads_data = order_service.load_ads()
orders_data = order_service.load_orders()
broadcast_thread: threading.Thread | None = None
broadcast_lock = threading.Lock()
gzov_thread: threading.Thread | None = None
gzov_lock = threading.Lock()
uploaded_message_photo: str | None = None
uploaded_message_photo_at: float = 0.0
uploaded_ad_photo_cache: dict[str, tuple[str, float]] = {}
failed_ad_photo_recoveries: dict[str, float] = {}
UPLOADED_PHOTO_MAX_AGE = 43200  # 12 часов

MAIN_MEDIA_DIR = BASE_DIR / "media" / "main"
MAIN_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
MAIN_PHOTO_LOCAL_PATH = MAIN_MEDIA_DIR / "main_photo.jpg"
MAIN_VIDEO_LOCAL_PATH = MAIN_MEDIA_DIR / "main_video.mp4"
MAIN_VIDEO_DOWNLOAD_TMP = MAIN_MEDIA_DIR / "main_video.download"
MAIN_VIDEO_DOC_CACHE_KEY = "main_video_doc_attachment"
MAIN_VIDEO_LOCAL_KEY = "main_video_local_path"
expiration_thread: threading.Thread | None = None
auto_broadcast_thread: threading.Thread | None = None
_last_admin_commands: dict[str, float] = {}
_ADMIN_COMMAND_TTL = 5.0
EXPIRY_WARNING_DAY = "expiry_warn_1d_at"
EXPIRY_WARNING_HOUR = "expiry_warn_1h_at"
EXPIRY_REMOVED_AT = "expired_removed_at"
last_broadcast_time = time.time()
pending_broadcast_after_quiet = False
BROADCAST_QUIET_START_HOUR = 23
BROADCAST_QUIET_END_HOUR = 7
service_send_cooldown_sec = float(runtime_data.get("service_send_cooldown_sec", 1.5) or 0)
# Интерактивные ответы не должны ждать старую паузу рассылки в 1.5 секунды.
# Массовые отправки используют отдельный лимитер ниже.
INTERACTIVE_SEND_COOLDOWN_CAP = max(0.0, float(os.getenv("VK_INTERACTIVE_SEND_COOLDOWN", "0.08") or 0.08))
_service_send_lock = threading.Lock()
_last_service_send_at = 0.0
_runtime_data_lock = threading.RLock()
_vk_sender_name_cache: dict[int, tuple[str, float]] = {}
_vk_sender_name_cache_lock = threading.RLock()
VK_SENDER_NAME_CACHE_TTL_SECONDS = 6 * 60 * 60
_chat_ids_lock = threading.RLock()
_chat_refresh_lock = threading.Lock()
_chat_refresh_inflight: set[int] = set()
CHAT_REGISTRY_REFRESH_TTL_SECONDS = 6 * 60 * 60
_media_upload_lock = threading.RLock()
_bulk_thread_local = threading.local()
_bulk_send_lock = threading.Lock()
_bulk_next_send_at = 0.0
BROADCAST_CHAT_WORKERS = max(1, min(12, int(os.getenv("VK_BROADCAST_WORKERS", "4") or 4)))
INFOZOV_WORKERS = max(1, min(12, int(os.getenv("VK_INFOZOV_WORKERS", "4") or 4)))
BACKGROUND_WORKERS = max(2, min(24, int(os.getenv("VK_BACKGROUND_WORKERS", "8") or 8)))
BULK_SEND_INTERVAL_SEC = max(0.03, float(os.getenv("VK_BULK_SEND_INTERVAL", "0.18") or 0.18))
INFOZOV_PENDING_BATCH = max(20, min(1000, int(os.getenv("VK_INFOZOV_BATCH", "200") or 200)))
PM_DIALOG_SYNC_TTL_SECONDS = 6 * 60 * 60
_background_executor = ThreadPoolExecutor(max_workers=BACKGROUND_WORKERS, thread_name_prefix="vk-bg")
APPLICATION_CAPTURE_WORKERS = max(4, min(32, int(os.getenv("VK_APPLICATION_WORKERS", "16") or 16)))
_application_executor = ThreadPoolExecutor(max_workers=APPLICATION_CAPTURE_WORKERS, thread_name_prefix="vk-app")
application_recovery_thread: threading.Thread | None = None
APPLICATION_RECOVERY_INTERVAL_SECONDS = max(10.0, min(300.0, float(os.getenv("VK_APPLICATION_RECOVERY_INTERVAL", "30") or 30)))
_application_capture_state_lock = threading.RLock()
from collections import OrderedDict
_processed_events: OrderedDict[str, float] = OrderedDict()
_PROCESSED_EVENTS_TTL = 300
_processed_events_lock = threading.Lock()


# ---------------- Поиск заявок о работе ----------------
applications_lock = APPLICATIONS_LOCK
applications_cleanup_thread: threading.Thread | None = None
APPLICATION_LIFETIME_SECONDS = 6 * 60 * 60
APPLICATION_DUPLICATE_SECONDS = 6 * 60 * 60  # не повторять одинаковое объявление 6 часов, даже из другого чата
JOB_FORWARD_BATCH_SIZE = 50
PM_JOBS_PAGE_SIZE = 5
COMPANIES_PAGE_SIZE = 8
pm_jobs_lock = PM_JOBS_LOCK
companies_lock = threading.RLock()
_users_db_lock = threading.RLock()
infozov_thread: threading.Thread | None = None
infozov_lock = threading.Lock()
WORK_CHAT_JOIN_LINK = "https://vk.me/join/3zqMOfkn01KYXy2cKXHRlrBUhPLnurRx1_0="
# Пользователь должен состоять в этой беседе, чтобы пользоваться каталогом в ЛС.
REQUIRED_PM_CHAT_ID = int(runtime_data.get("required_pm_chat_id", 2000000002) or 2000000002)
PM_MEMBERSHIP_CACHE_TTL_SECONDS = max(60, int(os.getenv("VK_PM_MEMBERSHIP_CACHE_TTL", "600") or 600))
PM_ACCESS_WORKERS = max(1, min(8, int(os.getenv("VK_PM_ACCESS_WORKERS", "4") or 4)))
# Пока есть пользователи, ожидающие вступления, делаем всего ОДИН запрос к
# списку участников обязательного чата раз в несколько секунд и проверяем всех
# ожидающих сразу. Это даёт автоматическое продолжение без кнопки «Проверить».
PM_AUTO_MEMBERSHIP_POLL_SECONDS = max(2.0, min(30.0, float(os.getenv("VK_PM_AUTO_MEMBERSHIP_POLL", "3") or 3)))
PM_AUTO_MEMBERSHIP_WAIT_TTL_SECONDS = max(300, int(os.getenv("VK_PM_AUTO_MEMBERSHIP_WAIT_TTL", str(24 * 60 * 60)) or 24 * 60 * 60))
_pm_membership_cache: dict[int, tuple[bool, float]] = {}
_pm_membership_lock = threading.RLock()
_pm_access_inflight: set[int] = set()
_pm_access_pending: dict[int, tuple[Callable[[], None], dict[str, Any]]] = {}
_pm_auto_membership_waiting: dict[int, tuple[dict[str, Any], float]] = {}
_pm_access_executor = ThreadPoolExecutor(max_workers=PM_ACCESS_WORKERS, thread_name_prefix="pm-access")
pm_membership_watch_thread: threading.Thread | None = None

chat_search_thread: threading.Thread | None = None
chat_search_lock = threading.Lock()
chat_search_state_lock = threading.RLock()
found_chats_db_lock = threading.RLock()
chat_search_cancel_event = threading.Event()
chat_search_runtime_lock = threading.RLock()
chat_search_runtime: dict[str, Any] = {
    "started_at": 0.0,
    "pages_checked": 0,
    "queries_checked": 0,
    "found": 0,
    "sent": 0,
    "current_query": "",
}


autojoin_thread: threading.Thread | None = None
autojoin_lock = threading.Lock()
autojoin_stop_event = threading.Event()
AUTOJOIN_COOLDOWN_SECONDS = 10.0


def get_vk_user_token() -> str:
    """Берёт user token из main.py; если строка пустая — из VK_USER_TOKEN."""
    inline_token = str(INLINE_VK_USER_TOKEN or "").strip()
    if inline_token:
        return inline_token
    return str(os.getenv("VK_USER_TOKEN") or "").strip()


VKCHATS_BASE_URL = "https://vkchats.ru"
VKCHATS_RESULT_LIMIT = 200

# Полный справочник городов/регионов подгружается один раз и кэшируется локально.
# Если GitHub временно недоступен, бот продолжит работать со встроенным списком.
RF_CITIES_DATA_URL = "https://raw.githubusercontent.com/arbaev/russia-cities/master/russia-cities.json"
RF_REGIONS_DATA_URL = "https://raw.githubusercontent.com/arbaev/russia-cities/master/russia-regions.json"
RF_GEO_CACHE_MAX_AGE_SEC = 30 * 24 * 60 * 60
_rf_geo_cache_memory: dict[str, Any] | None = None
_rf_geo_cache_lock = threading.RLock()
VKCHATS_HTTP_TIMEOUT = max(5.0, min(30.0, float(os.getenv("VKCHATS_HTTP_TIMEOUT", "12") or 12)))
VKCHATS_REQUEST_PAUSE = max(0.05, min(2.0, float(os.getenv("VKCHATS_REQUEST_PAUSE", "0.18") or 0.18)))
VKCHATS_STREAM_BATCH_SIZE = max(3, min(20, int(os.getenv("VKCHATS_STREAM_BATCH_SIZE", "10") or 10)))
VKCHATS_CARD_WORKERS = max(2, min(8, int(os.getenv("VKCHATS_CARD_WORKERS", "6") or 6)))
VKCHATS_PAGE_WORKERS = max(2, min(12, int(os.getenv("VKCHATS_PAGE_WORKERS", "8") or 8)))
VKCHATS_QUERY_WORKERS = max(2, min(12, int(os.getenv("VKCHATS_QUERY_WORKERS", "8") or 8)))

VKCHATS_WORK_KEYWORDS = [
    "Работа",
    "Рабочие",
    "Работники",
    "Подработка",
    "Подработки",
    "Вакансии",
    "Вакансия",
    "Вахта",
    "Вахтовики",
    "Калым",
    "Шабашка",
    "Шабашки",
    "Заработок",
    "Ищу работу",
    "Ищу подработку",
    "Работа сегодня",
    "Работа ежедневно",
    "Работа без опыта",
    "Работа с проживанием",
    "Работа склад",
    "Работа производство",
    "Работа завод",
    "Работа фабрика",
    "Работа стройка",
    "Работа строительство",
    "Работа грузчик",
    "Работа разнорабочий",
    "Работа комплектовщик",
    "Работа упаковщик",
    "Работа кладовщик",
    "Работа водитель",
    "Работа курьер",
    "Работа погрузчик",
    "Работа сварщик",
    "Работа монтажник",
    "Работа электрик",
    "Работа слесарь",
    "Работа оператор",
    "Работа рабочий",
    "Работа персонал",
    "Работа смена",
    "Работа подработка",
    "Работа вахта",
    "Временная работа",
    "Ежедневные выплаты",
    "Работа ежедневная оплата",
    "Срочно работа",
    "Срочная работа",
    "Срочно подработка",
    "Срочная подработка",
    "Требуются работники",
    "Требуются рабочие",
    "Требуются сотрудники",
    "Нужны работники",
    "Нужны рабочие",
    "Нужны сотрудники",
    "Ищем работников",
    "Ищем рабочих",
    "Ищем сотрудников",
    "Набор персонала",
    "Набор сотрудников",
    "Работа персонал",
    "Подбор персонала",
    "Работа рядом",
    "Подработка рядом",
    "Подработка сегодня",
    "Работа на сегодня",
    "Работа на день",
    "Подработка на день",
    "Работа по сменам",
    "Сменная работа",
    "Работа сменами",
    "Ночная работа",
    "Работа ночью",
    "Работа на выходные",
    "Подработка на выходные",
    "Работа выходного дня",
    "Оплата каждый день",
    "Оплата ежедневно",
    "Оплата после смены",
    "Ежедневная оплата работа",
    "Ежедневная оплата подработка",
    "Работа с обучением",
    "Работа без опыта вакансии",
    "Вакансии без опыта",
    "Работа с проживанием",
    "Вахта с проживанием",
    "Вахта работа с проживанием",
    "Работа на складе",
    "Подработка на складе",
    "Вакансии склад",
    "Склад вакансии",
    "Работа комплектовщик",
    "Работа сборщик",
    "Работа упаковщик",
    "Работа грузчик",
    "Грузчики работа",
    "Работа кладовщик",
    "Работа курьер",
    "Подработка курьер",
    "Курьеры работа",
    "Работа доставка",
    "Подработка доставка",
    "Работа логистика",
    "Работа производство",
    "Вакансии производство",
    "Производство вакансии",
    "Работа завод",
    "Завод вакансии",
    "Работа фабрика",
    "Фабрика вакансии",
    "Работа стройка",
    "Стройка вакансии",
    "Работа строительство",
    "Строительство вакансии",
    "Разнорабочие работа",
    "Работа разнорабочие",
    "Работа погрузка",
    "Работа комплектовка",
    "Работа упаковка",
    "Работа сборка заказов",

    # Английские запросы — на vkchats.ru встречаются беседы,
    # названные на английском или смешанно RU/EN.
    "Work",
    "Job",
    "Jobs",
    "Vacancy",
    "Vacancies",
    "Hiring",
    "We are hiring",
    "Workers",
    "Worker",
    "Staff",
    "Personnel",
    "Employment",
    "Job search",
    "Looking for work",
    "Looking for job",
    "Part time",
    "Part-time",
    "Part time job",
    "Side job",
    "Side hustle",
    "Temporary work",
    "Seasonal work",
    "Shift work",
    "Rotation work",
    "Daily pay",
    "Daily payment",
    "Weekly pay",
    "No experience",
    "No experience jobs",
    "Work with accommodation",
    "Jobs with accommodation",
    "Warehouse",
    "Warehouse jobs",
    "Warehouse worker",
    "Picker",
    "Order picker",
    "Packer",
    "Loader",
    "Mover",
    "General worker",
    "Production worker",
    "Factory",
    "Factory jobs",
    "Plant jobs",
    "Construction",
    "Construction jobs",
    "Construction worker",
    "Driver",
    "Driver jobs",
    "Courier",
    "Courier jobs",
    "Delivery jobs",
    "Forklift",
    "Forklift driver",
    "Welder",
    "Welder jobs",
    "Electrician",
    "Mechanic",
    "Operator",
    "Machine operator",
    "Assembler",
    "Cleaner",
    "Security jobs",
    "Handyman",
    "Laborer",
    "Labourer",
    "Recruitment",
    "Recruiting",
    "Job offers",
    "Work today",
    "Jobs today",
]


# Дополнительные поисковые профили. Каждый ПОЛНЫЙ запуск /поискчаты
# использует следующий профиль по кругу. Это не даёт поиску зациклиться
# на одном и том же наборе «Работа / Подработка / Вакансии».
# Если поиск остановлен вручную, номер runs не меняется — после запуска
# бот продолжит тот же профиль с сохранённой страницы.
VKCHATS_DISCOVERY_GROUPS: list[dict[str, Any]] = [
    {
        "name": "срочный найм и смены",
        "queries": [
            "Есть работа", "Предлагаю работу", "Нужны люди", "Требуются люди",
            "Срочно нужны люди", "Срочно требуются", "Выход на смену",
            "Нужны на смену", "Смена сегодня", "Смена завтра",
            "Подработка сегодня", "Подработка завтра", "Работа на сегодня",
            "Работа на завтра", "Работа на день", "Разовая работа",
            "Разовая подработка", "Временная работа", "Временная подработка",
            "Оплата за смену", "Оплата после смены", "Оплата сразу",
            "Ежедневная оплата", "Ежедневные выплаты", "Еженедельные выплаты",
            "Без опыта", "Работа без опыта", "Подработка без опыта",
        ],
        "city_templates": [
            "Нужны люди {geo}", "Требуются люди {geo}", "Смена сегодня {geo}",
            "Подработка сегодня {geo}", "Разовая работа {geo}",
            "Оплата за смену {geo}", "Работа без опыта {geo}",
        ],
        "region_templates": [
            "Нужны работники {geo}", "Срочно работа {geo}",
            "Подработка сегодня {geo}", "Работа без опыта {geo}",
        ],
    },
    {
        "name": "склады и логистика",
        "queries": [
            "Склад работа", "Склад подработка", "Склад вакансии", "Работа РЦ",
            "Распределительный центр работа", "Логистика работа", "Логистика вакансии",
            "Грузчики", "Грузчик", "Комплектовщики", "Комплектовщик",
            "Сборщики заказов", "Сборщик заказов", "Упаковщики", "Упаковщик",
            "Сортировщики", "Сортировщик", "Маркировщики", "Маркировщик",
            "Стикеровщики", "Стикеровщик", "Кладовщики", "Кладовщик",
            "Приемщики", "Приемщик", "Работник склада", "Сотрудник склада",
            "Водитель погрузчика", "Водитель штабелера", "Ричтрак",
            "Водитель ричтрака", "Карщик", "Погрузка разгрузка",
            "Комплектовка заказов", "Сборка заказов",
        ],
        "city_templates": [
            "Склад {geo}", "Грузчики {geo}", "Комплектовщики {geo}",
            "Сборщики заказов {geo}", "Упаковщики {geo}", "Кладовщики {geo}",
            "Водитель погрузчика {geo}", "Логистика {geo}",
        ],
        "region_templates": [
            "Работа склад {geo}", "Грузчики {geo}", "Комплектовщики {geo}",
            "Логистика {geo}", "РЦ работа {geo}",
        ],
    },
    {
        "name": "заводы и производство",
        "queries": [
            "Производство работа", "Производство вакансии", "Завод работа",
            "Завод вакансии", "Фабрика работа", "Фабрика вакансии",
            "Рабочий на производство", "Работники производства", "Оператор линии",
            "Оператор производства", "Оператор станка", "Оператор ЧПУ",
            "Сборщик на производство", "Упаковщик производство", "Контролер ОТК",
            "Слесари", "Слесарь", "Токари", "Токарь", "Фрезеровщики",
            "Фрезеровщик", "Сварщики", "Сварщик", "Электрики", "Электрик",
            "Механики", "Механик", "Машинисты", "Машинист",
            "Механизаторы", "Механизатор",
        ],
        "city_templates": [
            "Производство {geo}", "Завод {geo}", "Фабрика {geo}",
            "Оператор линии {geo}", "Сварщики {geo}", "Слесари {geo}",
            "Рабочие производство {geo}",
        ],
        "region_templates": [
            "Производство работа {geo}", "Завод работа {geo}",
            "Рабочие производство {geo}", "Сварщики {geo}",
        ],
    },
    {
        "name": "стройка и рабочие специальности",
        "queries": [
            "Стройка работа", "Строительство работа", "Стройка вакансии",
            "Разнорабочие", "Разнорабочий", "Подсобники", "Подсобный рабочий",
            "Рабочие на стройку", "Монтажники", "Монтажник", "Сварщики",
            "Сварщик", "Электрики", "Электрик", "Сантехники", "Сантехник",
            "Отделочники", "Отделочник", "Маляры", "Маляр", "Штукатуры",
            "Плиточники", "Арматурщики", "Бетонщики", "Каменщики",
            "Плотники", "Кровельщики", "Фасадчики", "Дорожные рабочие",
        ],
        "city_templates": [
            "Стройка {geo}", "Разнорабочие {geo}", "Подсобники {geo}",
            "Монтажники {geo}", "Сварщики {geo}", "Электрики {geo}",
            "Рабочие на стройку {geo}",
        ],
        "region_templates": [
            "Стройка работа {geo}", "Разнорабочие {geo}",
            "Монтажники {geo}", "Рабочие на стройку {geo}",
        ],
    },
    {
        "name": "доставка, торговля и сервис",
        "queries": [
            "Курьеры", "Курьер", "Пеший курьер", "Автокурьер", "Велокурьер",
            "Доставка работа", "Доставка подработка", "Водители", "Водитель",
            "Водитель категории B", "Водитель категории C", "Водитель категории CE",
            "Экспедиторы", "Экспедитор", "Мерчандайзеры", "Мерчандайзер",
            "Промоутеры", "Промоутер", "Кассиры", "Кассир", "Продавцы",
            "Продавец", "Работник торгового зала", "Сотрудник торгового зала",
            "Охранники", "Охрана работа", "Уборщики", "Уборщицы", "Клининг работа",
            "ПВЗ работа", "Маркетплейс работа",
        ],
        "city_templates": [
            "Курьеры {geo}", "Доставка {geo}", "Водители {geo}",
            "Мерчандайзер {geo}", "Промоутеры {geo}", "Кассиры {geo}",
            "ПВЗ работа {geo}", "Маркетплейс работа {geo}",
        ],
        "region_templates": [
            "Курьеры {geo}", "Доставка работа {geo}", "Водители {geo}",
            "Работа ПВЗ {geo}",
        ],
    },
    {
        "name": "вахта и проживание",
        "queries": [
            "Вахта", "Работа вахтой", "Вахтовая работа", "Вахтовики",
            "Вахта вакансии", "Вахта с проживанием", "Работа с проживанием",
            "Работа с жильем", "Работа жилье", "Работа общежитие",
            "Вахта питание проживание", "Вахта проезд проживание",
            "Вахта склад", "Вахта производство", "Вахта завод", "Вахта стройка",
            "Вахта разнорабочие", "Вахта грузчики", "Вахта комплектовщики",
            "Вахта сварщики", "Вахта монтажники", "Вахта водители",
            "Север вахта", "Работа на севере", "Вахта север",
        ],
        "city_templates": [
            "Вахта {geo}", "Работа с проживанием {geo}", "Вахта склад {geo}",
            "Вахта производство {geo}", "Вахта стройка {geo}",
            "Вахта разнорабочие {geo}",
        ],
        "region_templates": [
            "Вахта {geo}", "Работа с проживанием {geo}",
            "Вахта производство {geo}", "Вахта стройка {geo}",
        ],
    },
    {
        "name": "разговорные запросы",
        "queries": [
            "Шабашка", "Шабашки", "Калым", "Калымы", "Халтура", "Халтурка",
            "Подработка на час", "Подработка на пару часов", "Работа на пару часов",
            "Разовая шабашка", "Нужна подработка", "Ищу подработку",
            "Кто ищет работу", "Кому нужна работа", "Есть вакансии",
            "Есть подработка", "Нужны рабочие руки", "Рабочие руки",
            "Подработка выходные", "Подработка вечером", "Подработка ночью",
            "Ночная смена работа", "Дневная смена работа",
            "Свободные смены", "Смены подработка", "Биржа подработки",
        ],
        "city_templates": [
            "Шабашка {geo}", "Калым {geo}", "Халтура {geo}",
            "Есть подработка {geo}", "Нужны рабочие руки {geo}",
            "Свободные смены {geo}", "Подработка выходные {geo}",
        ],
        "region_templates": [
            "Шабашка {geo}", "Калым {geo}", "Халтура {geo}",
            "Есть подработка {geo}",
        ],
    },
]


# Строгий белый список слов для ручной очереди вступления.
# Сомнительные чаты (барахолки, знакомства, купля/продажа и т.п.) не показываются.
WORK_JOIN_TITLE_TERMS = (
    "работа", "подработка", "подработки", "вакансия", "вакансии",
    "вахта", "вахтовики", "заработок", "калым", "шабашка", "шабашки",
    "рабочие", "работники", "персонал", "сотрудники", "найм",
    "склад", "склады", "производство", "завод", "фабрика",
    "стройка", "строительство", "грузчик", "грузчики",
    "разнорабочий", "разнорабочие", "комплектовщик", "комплектовщики",
    "упаковщик", "упаковщики", "кладовщик", "кладовщики",
    "водитель", "водители", "курьер", "курьеры",
    "погрузчик", "сварщик", "сварщики", "монтажник", "монтажники",
    "электрик", "электрики", "слесарь", "слесари",
    "оператор", "операторы", "смена", "смены",
    "ежедневные выплаты", "ежедневная оплата",
    "work", "job", "jobs", "vacancy", "vacancies", "hiring",
    "workers", "staff", "employment", "part time", "part-time",
    "warehouse", "factory", "construction", "loader", "picker",
    "packer", "driver", "courier", "welder",
    "халтура", "халтурка", "калымы", "подсобник", "подсобники",
    "сборщик", "сборщики", "сортировщик", "сортировщики",
    "маркировщик", "маркировщики", "стикеровщик", "стикеровщики",
    "приемщик", "приемщики", "ричтрак", "карщик", "карщики",
    "логистика", "рц", "распределительный центр", "доставка",
    "мерчандайзер", "мерчандайзеры", "промоутер", "промоутеры",
    "кассир", "кассиры", "продавец", "продавцы", "торговый зал",
    "охрана", "охранник", "охранники", "уборщик", "уборщики",
    "клининг", "пвз", "маркетплейс", "сантехник", "сантехники",
    "отделочник", "отделочники", "маляр", "маляры", "штукатур",
    "плиточник", "арматурщик", "бетонщик", "каменщик", "плотник",
    "кровельщик", "фасадчик", "токарь", "токари", "фрезеровщик",
    "чпу", "контролер отк", "механизатор", "механизаторы",
    "экспедитор", "экспедиторы", "вахтовая работа", "рабочие руки",
)

WORK_JOIN_BLOCK_TERMS = (
    "барахолка", "барахолки", "куплю", "продам", "продажа", "покупка",
    "объявления", "маркет", "market", "доска объявлений",
    "знакомства", "чат знакомств", "флуд", "болталка", "мемы",
)


def _normalize_join_title(value: str) -> str:
    title = html.unescape(str(value or "")).lower().replace("ё", "е")
    title = re.sub(r"[^a-zа-я0-9]+", " ", title, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", title).strip()


def _is_strict_work_join_title(value: str) -> bool:
    """True только для названий, которые явно относятся к работе."""
    title = _normalize_join_title(value)
    if not title:
        return False

    for blocked in WORK_JOIN_BLOCK_TERMS:
        blocked_norm = _normalize_join_title(blocked)
        if blocked_norm and blocked_norm in title:
            return False

    for term in WORK_JOIN_TITLE_TERMS:
        term_norm = _normalize_join_title(term)
        if not term_norm:
            continue
        if re.search(
            rf"(?<![a-zа-я0-9]){re.escape(term_norm)}(?![a-zа-я0-9])",
            title,
            flags=re.IGNORECASE,
        ):
            return True

    return False


def build_manual_join_keyboard(link: str) -> str:
    return build_tracked_openlink_keyboard(
        "➡️ Открыть чат",
        link,
        stats_key="manual_join_open_chat",
        stats_source="Админ / найденные чаты",
        color=VkKeyboardColor.PRIMARY,
    )


def _manual_join_queue_worker(chat_id: int) -> None:
    """Мгновенно показывает только строго проверенные рабочие чаты.

    Вступление остаётся ручным, но ожидания по 10 секунд больше нет:
    ссылки выдаются пачками сразу.
    """
    try:
        state = _load_chat_search_state()
        seen_map = state.get("seen") or {}
        if not isinstance(seen_map, dict):
            seen_map = {}

        candidates: list[tuple[str, str, int | None]] = []
        dedupe: set[str] = set()

        for raw_link, meta in seen_map.items():
            if not isinstance(meta, dict):
                continue

            link = _normalize_vk_join_link(str(raw_link or ""))
            key = _vk_join_dedupe_key(link)
            if not link or not key or key in dedupe:
                continue

            title = " ".join(str(meta.get("title") or "").split()).strip()
            if not _is_strict_work_join_title(title):
                continue

            members = meta.get("members")
            if not isinstance(members, int):
                members = None

            dedupe.add(key)
            candidates.append((title, link, members))

        if not candidates:
            send_message(
                chat_id,
                "ℹ️ В сохранённых результатах нет чатов, прошедших строгую проверку названия.",
                rate_limit=False,
            )
            return

        send_message(
            chat_id,
            f"✅ Строгую проверку прошли {len(candidates)} чатов.\n"
            "Показываю их сразу, без задержки.",
            rate_limit=False,
        )

        # По 10 чатов в одном сообщении, чтобы не создавать сотни сообщений.
        for batch_start in range(0, len(candidates), 10):
            batch = candidates[batch_start:batch_start + 10]
            blocks: list[str] = []

            for local_index, (title, link, members) in enumerate(batch, start=batch_start + 1):
                lines = [f"{local_index}. {title}"]
                if isinstance(members, int):
                    lines.append(f"👥 {members}")
                lines.append(link)
                blocks.append("\n".join(lines))

            send_message(
                chat_id,
                "\n\n".join(blocks),
                rate_limit=False,
            )

        send_message(
            chat_id,
            f"✅ Готово. Проверенных рабочих чатов: {len(candidates)}.",
            rate_limit=False,
        )

    except Exception as exc:
        logger.exception("Ошибка быстрой выдачи рабочих чатов: %s", exc)
        send_message(chat_id, f"❌ Ошибка очереди: {exc}", rate_limit=False)


def start_manual_join_queue(chat_id: int) -> None:
    threading.Thread(
        target=_manual_join_queue_worker,
        args=(int(chat_id),),
        name="manual-work-chat-queue",
        daemon=True,
    ).start()


def _load_autojoin_state() -> dict[str, Any]:
    data = read_json(
        AUTOJOIN_STATE_PATH,
        {"processed": {}, "joined": 0, "skipped": 0, "errors": 0},
    )
    if not isinstance(data, dict):
        data = {"processed": {}, "joined": 0, "skipped": 0, "errors": 0}
    if not isinstance(data.get("processed"), dict):
        data["processed"] = {}
    data.setdefault("joined", 0)
    data.setdefault("skipped", 0)
    data.setdefault("errors", 0)
    return data


def _save_autojoin_state(data: dict[str, Any]) -> None:
    write_json_atomic(AUTOJOIN_STATE_PATH, data)


def _extract_chat_preview_title(response: Any) -> tuple[str, bool]:
    """Возвращает (title, already_joined) из messages.getChatPreview."""
    if not isinstance(response, dict):
        return "", False

    preview = response.get("preview")
    if not isinstance(preview, dict):
        # На случай другой обёртки/версии ответа.
        nested = response.get("response")
        if isinstance(nested, dict):
            preview = nested.get("preview")
    if not isinstance(preview, dict):
        return "", False

    title = " ".join(str(preview.get("title") or "").split()).strip()
    already_joined = bool(
        preview.get("joined")
        or preview.get("is_member")
    )
    return title, already_joined


def _build_autojoin_candidates() -> list[tuple[str, str]]:
    """Берёт сохранённые ссылки поиска, без изменения /поискчаты."""
    search_state = _load_chat_search_state()
    seen_map = search_state.get("seen") or {}
    if not isinstance(seen_map, dict):
        return []

    result: list[tuple[str, str]] = []
    dedupe: set[str] = set()

    for raw_link, meta in seen_map.items():
        link = _normalize_vk_join_link(str(raw_link or ""))
        key = _vk_join_dedupe_key(link)
        if not link or not key or key in dedupe:
            continue

        stored_title = ""
        if isinstance(meta, dict):
            stored_title = " ".join(str(meta.get("title") or "").split()).strip()

        # Первичный фильтр по сохранённому названию.
        # Перед реальным входом всё равно будет второй, более важный,
        # getChatPreview-проверка актуального названия через user token.
        if stored_title and not _is_strict_work_join_title(stored_title):
            continue

        dedupe.add(key)
        result.append((stored_title, link))

    return result


def _autojoin_worker(chat_id: int) -> None:
    global autojoin_thread

    token = get_vk_user_token()
    if not token:
        send_message(
            chat_id,
            "❌ Автовход не запущен: в .env нет VK_USER_TOKEN.",
            rate_limit=False,
        )
        with autojoin_lock:
            autojoin_thread = None
        return

    try:
        user_session = vk_api.VkApi(token=token)
        user_vk = user_session.get_api()

        # Проверяем, что токен действительно пользовательский и рабочий.
        try:
            me = user_vk.users.get()
            if not me:
                raise RuntimeError("users.get вернул пустой ответ")
            account_id = int(me[0].get("id") or 0)
            account_name = (
                f"{me[0].get('first_name', '')} {me[0].get('last_name', '')}"
            ).strip()
        except Exception as exc:
            send_message(
                chat_id,
                f"❌ VK_USER_TOKEN не работает: {exc}",
                rate_limit=False,
            )
            return

        candidates = _build_autojoin_candidates()
        state = _load_autojoin_state()
        processed: dict[str, Any] = state.setdefault("processed", {})

        # Не обрабатываем повторно успешно вступлённые/отфильтрованные ссылки.
        pending: list[tuple[str, str]] = []
        for stored_title, link in candidates:
            key = _vk_join_dedupe_key(link)
            previous = processed.get(key)
            if isinstance(previous, dict) and previous.get("status") in {
                "joined",
                "already_joined",
                "blocked_title",
                "invalid_title",
            }:
                continue
            pending.append((stored_title, link))

        if not pending:
            send_message(
                chat_id,
                "ℹ️ Для автовхода нет новых проверенных ссылок.",
                rate_limit=False,
            )
            return

        autojoin_stop_event.clear()

        send_message(
            chat_id,
            "🚪 Автовход запущен.\n"
            f"Аккаунт: [id{account_id}|{account_name or account_id}]\n"
            f"Ссылок в очереди: {len(pending)}\n"
            "Пауза между входами: 10 секунд.\n\n"
            "Перед КАЖДЫМ входом бот получает актуальное название чата через VK "
            "и повторно прогоняет его через строгий рабочий фильтр.",
            rate_limit=False,
        )

        joined_this_run = 0
        already_this_run = 0
        skipped_this_run = 0
        errors_this_run = 0

        for index, (stored_title, link) in enumerate(pending, start=1):
            if autojoin_stop_event.is_set():
                break

            key = _vk_join_dedupe_key(link)
            if not key:
                continue

            # 1. Актуальное название непосредственно у VK до вступления.
            try:
                preview_response = user_vk.messages.getChatPreview(link=link)
                actual_title, already_joined = _extract_chat_preview_title(preview_response)
            except Exception as exc:
                errors_this_run += 1
                state["errors"] = int(state.get("errors") or 0) + 1
                processed[key] = {
                    "status": "preview_error",
                    "link": link,
                    "stored_title": stored_title,
                    "error": str(exc),
                    "at": now_msk_str(),
                }
                _save_autojoin_state(state)
                logger.warning("Автовход: getChatPreview %s: %s", link, exc)
                # На preview-ошибке НЕ вступаем.
                continue

            # 2. Если уже состоим — просто запоминаем.
            if already_joined:
                already_this_run += 1
                processed[key] = {
                    "status": "already_joined",
                    "link": link,
                    "title": actual_title or stored_title,
                    "at": now_msk_str(),
                }
                _save_autojoin_state(state)
                continue

            # 3. Жёсткая проверка РЕАЛЬНОГО названия.
            if not actual_title:
                skipped_this_run += 1
                state["skipped"] = int(state.get("skipped") or 0) + 1
                processed[key] = {
                    "status": "invalid_title",
                    "link": link,
                    "stored_title": stored_title,
                    "at": now_msk_str(),
                }
                _save_autojoin_state(state)
                logger.info("Автовход: пропуск без названия %s", link)
                continue

            if not _is_strict_work_join_title(actual_title):
                skipped_this_run += 1
                state["skipped"] = int(state.get("skipped") or 0) + 1
                processed[key] = {
                    "status": "blocked_title",
                    "link": link,
                    "title": actual_title,
                    "at": now_msk_str(),
                }
                _save_autojoin_state(state)
                logger.info(
                    "Автовход: чат НЕ рабочий, вход отменён: %r | %s",
                    actual_title,
                    link,
                )
                continue

            # 4. Название прошло фильтр -> вступаем.
            try:
                join_response = user_vk.messages.joinChatByInviteLink(link=link)

                joined_this_run += 1
                state["joined"] = int(state.get("joined") or 0) + 1
                processed[key] = {
                    "status": "joined",
                    "link": link,
                    "title": actual_title,
                    "response": join_response,
                    "at": now_msk_str(),
                }
                _save_autojoin_state(state)

                logger.info(
                    "Автовход: УСПЕХ %s/%s: %r | %s",
                    index,
                    len(pending),
                    actual_title,
                    link,
                )

            except Exception as exc:
                errors_this_run += 1
                state["errors"] = int(state.get("errors") or 0) + 1
                processed[key] = {
                    "status": "join_error",
                    "link": link,
                    "title": actual_title,
                    "error": str(exc),
                    "at": now_msk_str(),
                }
                _save_autojoin_state(state)

                logger.warning(
                    "Автовход: ошибка вступления %r | %s: %s",
                    actual_title,
                    link,
                    exc,
                )

            # КД именно между попытками реального вступления.
            if index < len(pending) and not autojoin_stop_event.is_set():
                autojoin_stop_event.wait(AUTOJOIN_COOLDOWN_SECONDS)

        status = "⏹ Остановлен" if autojoin_stop_event.is_set() else "✅ Завершён"
        send_message(
            chat_id,
            f"{status} автовход.\n\n"
            f"✅ Вступил: {joined_this_run}\n"
            f"ℹ️ Уже состоял: {already_this_run}\n"
            f"🚫 Отфильтровано: {skipped_this_run}\n"
            f"⚠️ Ошибок: {errors_this_run}",
            rate_limit=False,
        )

    except Exception as exc:
        logger.exception("Критическая ошибка автовхода: %s", exc)
        send_message(
            chat_id,
            f"❌ Автовход завершился с ошибкой: {exc}",
            rate_limit=False,
        )
    finally:
        autojoin_stop_event.clear()
        with autojoin_lock:
            autojoin_thread = None


def start_autojoin(chat_id: int) -> bool:
    global autojoin_thread

    with autojoin_lock:
        if autojoin_thread is not None and autojoin_thread.is_alive():
            return False

        autojoin_stop_event.clear()
        autojoin_thread = threading.Thread(
            target=_autojoin_worker,
            args=(int(chat_id),),
            name="vk-user-autojoin",
            daemon=True,
        )
        autojoin_thread.start()
        return True


def stop_autojoin() -> bool:
    with autojoin_lock:
        running = autojoin_thread is not None and autojoin_thread.is_alive()
        if running:
            autojoin_stop_event.set()
        return running


def reset_autojoin_errors() -> int:
    """Разрешает повторно попробовать ссылки, где раньше была API-ошибка."""
    state = _load_autojoin_state()
    processed = state.get("processed") or {}
    if not isinstance(processed, dict):
        return 0

    removed = 0
    for key in list(processed.keys()):
        item = processed.get(key)
        if isinstance(item, dict) and item.get("status") in {
            "preview_error",
            "join_error",
        }:
            processed.pop(key, None)
            removed += 1

    state["processed"] = processed
    _save_autojoin_state(state)
    return removed



VKCHATS_CITIES = [
    "Москва",
    "Московская область",
    "Балашиха",
    "Подольск",
    "Химки",
    "Мытищи",
    "Королёв",
    "Люберцы",
    "Красногорск",
    "Одинцово",
    "Домодедово",
    "Пушкино",
    "Щёлково",
    "Раменское",
    "Сергиев Посад",
    "Коломна",
    "Серпухов",
    "Орехово-Зуево",
    "Ногинск",
    "Электросталь",
    "Санкт-Петербург",
    "Ленинградская область",
    "Гатчина",
    "Выборг",
    "Всеволожск",
    "Новосибирск",
    "Новосибирская область",
    "Екатеринбург",
    "Свердловская область",
    "Казань",
    "Татарстан",
    "Нижний Новгород",
    "Нижегородская область",
    "Челябинск",
    "Челябинская область",
    "Самара",
    "Самарская область",
    "Тольятти",
    "Омск",
    "Омская область",
    "Ростов-на-Дону",
    "Ростовская область",
    "Таганрог",
    "Шахты",
    "Уфа",
    "Башкортостан",
    "Красноярск",
    "Красноярский край",
    "Пермь",
    "Пермский край",
    "Воронеж",
    "Воронежская область",
    "Волгоград",
    "Волгоградская область",
    "Краснодар",
    "Краснодарский край",
    "Сочи",
    "Новороссийск",
    "Армавир",
    "Саратов",
    "Саратовская область",
    "Тюмень",
    "Тюменская область",
    "Ижевск",
    "Удмуртия",
    "Барнаул",
    "Алтайский край",
    "Иркутск",
    "Иркутская область",
    "Хабаровск",
    "Хабаровский край",
    "Владивосток",
    "Приморский край",
    "Калуга",
    "Калужская область",
    "Обнинск",
    "Тула",
    "Тульская область",
    "Рязань",
    "Рязанская область",
    "Ярославль",
    "Ярославская область",
    "Тамбов",
    "Тамбовская область",
    "Курск",
    "Курская область",
    "Белгород",
    "Белгородская область",
    "Брянск",
    "Брянская область",
    "Орёл",
    "Орловская область",
    "Липецк",
    "Липецкая область",
    "Пенза",
    "Пензенская область",
    "Ульяновск",
    "Ульяновская область",
    "Оренбург",
    "Оренбургская область",
    "Киров",
    "Кировская область",
    "Чебоксары",
    "Чувашия",
    "Йошкар-Ола",
    "Марий Эл",
    "Саранск",
    "Мордовия",
    "Астрахань",
    "Астраханская область",
    "Ставрополь",
    "Ставропольский край",
    "Пятигорск",
    "Ессентуки",
    "Махачкала",
    "Дагестан",
    "Владикавказ",
    "Грозный",
    "Нальчик",
    "Кемерово",
    "Кемеровская область",
    "Новокузнецк",
    "Томск",
    "Томская область",
    "Абакан",
    "Хакасия",
    "Кызыл",
    "Улан-Удэ",
    "Бурятия",
    "Чита",
    "Забайкальский край",
    "Якутск",
    "Саха Якутия",
    "Благовещенск",
    "Амурская область",
    "Южно-Сахалинск",
    "Сахалинская область",
    "Петропавловск-Камчатский",
    "Мурманск",
    "Мурманская область",
    "Архангельск",
    "Архангельская область",
    "Вологда",
    "Вологодская область",
    "Череповец",
    "Псков",
    "Псковская область",
    "Великий Новгород",
    "Новгородская область",
    "Калининград",
    "Калининградская область",
    "Петрозаводск",
    "Карелия",
    "Сыктывкар",
    "Коми",
    "Смоленск",
    "Смоленская область",
    "Тверь",
    "Тверская область",
    "Кострома",
    "Костромская область",
    "Владимир",
    "Владимирская область",
    "Иваново",
    "Ивановская область",
    "Набережные Челны",
    "Нижнекамск",
    "Магнитогорск",
    "Сургут",
    "Нижневартовск",
    "Ханты-Мансийск",
    "Новый Уренгой",
    "Ноябрьск",
    "ЯНАО",
    "ХМАО",
]




class _VkChatsSearchParser(HTMLParser):
    """Вытаскивает карточки /chat/... и приглашения vk.me/join из выдачи vkchats.ru."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.cards: list[dict[str, Any]] = []
        self._current_href: str | None = None
        self._current_title: list[str] = []
        self._current_text: list[str] = []
        self._inside_chat_anchor = False

    @staticmethod
    def _is_chat_href(href: str) -> bool:
        return href.startswith("/chat/") or "vkchats.ru/chat/" in href

    def _flush(self) -> None:
        if not self._current_href:
            return
        title = " ".join(" ".join(self._current_title).split()).strip()
        body = " ".join(" ".join(self._current_text).split()).strip()
        self.cards.append(
            {
                "detail_url": urljoin(VKCHATS_BASE_URL, self._current_href),
                "title": title,
                "text": body,
            }
        )
        self._current_href = None
        self._current_title = []
        self._current_text = []
        self._inside_chat_anchor = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attrs_map = {str(k).lower(): (v or "") for k, v in attrs}
        href = attrs_map.get("href", "")
        if href and self._is_chat_href(href):
            self._flush()
            self._current_href = href
            self._inside_chat_anchor = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._inside_chat_anchor:
            self._inside_chat_anchor = False

    def handle_data(self, data: str) -> None:
        if not self._current_href:
            return
        clean = " ".join(str(data or "").split())
        if not clean:
            return
        self._current_text.append(clean)
        if self._inside_chat_anchor:
            self._current_title.append(clean)

    def close(self) -> None:
        super().close()
        self._flush()


_VK_JOIN_RE = re.compile(r"https?://vk\.me/join/", re.IGNORECASE)


_VKCHATS_MEMBER_PATTERNS = (
    re.compile(r"Участников\s*:\s*([\d\s\u00a0]+)", re.IGNORECASE),
    re.compile(r"В\s+чате\s+([\d\s\u00a0]+)\s+участник", re.IGNORECASE),
    re.compile(r"([\d\s\u00a0]+)\s+участник(?:ов|а)?", re.IGNORECASE),
)


def _extract_vkchats_member_count(value: str) -> int | None:
    clean = html.unescape(str(value or ""))
    for pattern in _VKCHATS_MEMBER_PATTERNS:
        match = pattern.search(clean)
        if not match:
            continue
        digits = re.sub(r"\D+", "", match.group(1))
        if not digits:
            continue
        try:
            return max(0, int(digits))
        except (TypeError, ValueError):
            pass
    return None



def _normalize_vk_join_link(value: str) -> str:
    """Возвращает только саму vk.me/join ссылку без HTML/JSON после неё.

    Валидные варианты:
      https://vk.me/join/ABC123
      https://vk.me/join/ABC/DEF
      https://vk.me/join/ABC/DEF=

    "=" добавляется только если он стоит СРАЗУ после токена.
    Поэтому конструкции вроде:
      ...ABC"/><meta name=
      ...ABC","image":"https://...
    больше никогда не превращаются в часть ссылки.
    """
    raw = html.unescape(str(value or "")).strip()
    prefix = _VK_JOIN_RE.search(raw)
    if not prefix:
        return ""

    tail = raw[prefix.end():]
    if not tail:
        return ""

    # Сам токен. "%" намеренно не разрешён: %2F, %5Cn и подобное
    # считаем уже посторонним хвостом, как и просил пользователь.
    token_match = re.match(r"[A-Za-z0-9_+\-/]+", tail)
    if not token_match:
        return ""

    token = token_match.group(0)
    end_pos = token_match.end()

    # Если "=" относится к самой ссылке, он находится сразу после токена.
    if end_pos < len(tail) and tail[end_pos] == "=":
        token += "="

    return f"https://vk.me/join/{token}"



def _vk_join_dedupe_key(value: str) -> str:
    link = _normalize_vk_join_link(value)
    if not link:
        return ""

    token = link.split("/join/", 1)[-1]
    # Один и тот же invite иногда встречается на сайте и с "=" на конце,
    # и без него. Для отображения URL не меняем, а для сравнения "=" убираем.
    return token.rstrip("=")


def _dedupe_chat_search_results(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Последняя защитная дедупликация прямо перед выводом."""
    unique: list[dict[str, Any]] = []
    positions: dict[str, int] = {}

    for item in items:
        link = _normalize_vk_join_link(str(item.get("link") or ""))
        key = _vk_join_dedupe_key(link)
        if not key:
            continue

        prepared = dict(item)
        prepared["link"] = link

        if key not in positions:
            positions[key] = len(unique)
            unique.append(prepared)
            continue

        # Если дубль содержит лучшее название/число участников,
        # обновляем уже сохранённую запись, а вторую не выводим.
        current = unique[positions[key]]

        old_title = " ".join(str(current.get("title") or "").split()).strip()
        new_title = " ".join(str(prepared.get("title") or "").split()).strip()
        old_bad = not old_title or old_title.lower() in {"без названия", "чат"}
        new_good = bool(new_title) and new_title.lower() not in {"без названия", "чат"}
        if old_bad and new_good:
            current["title"] = new_title

        old_members = current.get("members")
        new_members = prepared.get("members")
        if not isinstance(old_members, int) and isinstance(new_members, int):
            current["members"] = new_members

        # Если один вариант URL имеет "=" на конце, предпочитаем его для вывода.
        if str(prepared.get("link") or "").endswith("="):
            current["link"] = prepared["link"]

    return unique


def _extract_vk_join_links(value: str) -> list[str]:
    raw = html.unescape(str(value or ""))
    result: list[str] = []
    seen_local: set[str] = set()

    # Каждое вхождение обрабатываем отдельно. До следующей ссылки можно
    # захватить текст, normalizer сам остановится на конце токена.
    matches = list(_VK_JOIN_RE.finditer(raw))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        candidate = raw[match.start():end]
        link = _normalize_vk_join_link(candidate)
        key = _vk_join_dedupe_key(link)
        if not link or not key or key in seen_local:
            continue
        seen_local.add(key)
        result.append(link)

    return result


class _VkChatsVisibleDetailParser(HTMLParser):
    """Берёт только то, что реально присутствует в видимой части detail-страницы.

    Это позволяет находить дополнительные vk.me/join из описания карточки,
    но не считать приглашения из <script>, Next.js JSON, шаблонов и прочего
    скрытого служебного payload.
    """

    _SKIP_TAGS = {"script", "style", "noscript", "template"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._text_parts: list[str] = []
        self._hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = str(tag or "").lower()
        if tag in self._SKIP_TAGS:
            self._skip_depth += 1
            return

        if self._skip_depth:
            return

        if tag == "a":
            attrs_map = {str(k).lower(): (v or "") for k, v in attrs}
            href = html.unescape(str(attrs_map.get("href") or "")).strip()
            if "vk.me/join/" in href.lower():
                self._hrefs.append(href)

    def handle_endtag(self, tag: str) -> None:
        tag = str(tag or "").lower()
        if tag in self._SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        clean = " ".join(str(data or "").split()).strip()
        if clean:
            self._text_parts.append(clean)

    def get_join_links(self) -> list[str]:
        # href нужен отдельно: браузер может показывать анкор вроде «вступить»,
        # а сама vk.me/join ссылка будет только в атрибуте href.
        visible_blob = "\n".join(self._text_parts + self._hrefs)
        return _extract_vk_join_links(visible_blob)


def _extract_vkchats_visible_detail_links(page_html: str) -> list[str]:
    if not page_html:
        return []
    parser = _VkChatsVisibleDetailParser()
    try:
        parser.feed(str(page_html))
        parser.close()
    except Exception:
        logger.exception("Ошибка разбора видимого описания карточки vkchats")
        return []
    return parser.get_join_links()



def _decode_vkchats_title(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    # В Next/JSON данные часто лежат как \"Название\".
    try:
        decoded = json.loads('"' + raw.replace('"', '\\"') + '"')
    except Exception:
        decoded = raw

    decoded = html.unescape(str(decoded))
    decoded = decoded.replace("\\/", "/").replace('\\"', '"')
    decoded = " ".join(decoded.split()).strip()

    # Не принимаем технические значения за название чата.
    low = decoded.lower()
    if (
        not decoded
        or decoded.startswith(("http://", "https://"))
        or "vk.me/join/" in low
        or low in {"chat", "чат", "vkchats", "vk chats"}
    ):
        return ""

    return decoded[:220]


_FALLBACK_TITLE_PATTERNS = (
    # owner_group / owner_user в сериализованных данных сайта.
    re.compile(
        r'owner_(?:group|user).{0,1200}?(?:\\?")title(?:\\?")\s*:\s*(?:\\?")([^"\r\n]{2,260})',
        re.IGNORECASE | re.DOTALL,
    ),
    # Обычное JSON-поле title рядом с приглашением.
    re.compile(
        r'(?:\\?")title(?:\\?")\s*:\s*(?:\\?")([^"\r\n]{2,260})',
        re.IGNORECASE,
    ),
    # HTML title/card heading.
    re.compile(
        r'<(?:h1|h2|h3)[^>]*>(.*?)</(?:h1|h2|h3)>',
        re.IGNORECASE | re.DOTALL,
    ),
)



def _extract_vkchats_detail_title(page_html: str) -> str:
    raw = html.unescape(str(page_html or ""))

    # На странице чата название точно есть в <h1>.
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", raw, re.IGNORECASE | re.DOTALL)
    if h1:
        title = re.sub(r"<[^>]+>", " ", h1.group(1))
        title = " ".join(title.split()).strip()
        if title:
            return title[:220]

    # Запасной вариант — <title>.
    title_tag = re.search(r"<title[^>]*>(.*?)</title>", raw, re.IGNORECASE | re.DOTALL)
    if title_tag:
        title = re.sub(r"<[^>]+>", " ", title_tag.group(1))
        title = " ".join(title.split()).strip()

        # Типичный title сайта:
        # Чат "Название" | 254 участника | Создатель: ...
        match = re.search(
            r'^\s*Чат\s+["«](.+?)["»]\s*(?:\||$)',
            title,
            re.IGNORECASE,
        )
        if match:
            return " ".join(match.group(1).split()).strip()[:220]

        # Если формат сайта поменялся — всё равно лучше title, чем "Чат".
        title = re.sub(r"\s*[-|]\s*VKChats.*$", "", title, flags=re.IGNORECASE)
        title = title.strip()
        if title:
            return title[:220]

    return ""


def _find_vkchats_detail_url_near(page_html: str, position: int) -> str:
    raw = str(page_html or "")
    left = max(0, int(position) - 5000)
    right = min(len(raw), int(position) + 5000)
    chunk = raw[left:right]
    local_position = int(position) - left

    candidates: list[tuple[int, str]] = []

    for match in re.finditer(
        r'(?:https?://vkchats\.ru)?(/chat/[A-Za-z0-9_-]{8,})',
        chunk,
        re.IGNORECASE,
    ):
        href = match.group(1)
        distance = abs(match.start() - local_position)
        candidates.append((distance, urljoin(VKCHATS_BASE_URL, href)))

    if not candidates:
        return ""

    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _resolve_vkchats_card_title(card: dict[str, Any], request_budget: list[int]) -> str:
    current = " ".join(str(card.get("title") or "").split()).strip()
    if current and current.lower() not in {"без названия", "чат", "открыть чат", ">"}:
        return current[:220]

    # Если карточка ведёт на /chat/... — берём название из <title>/<h1>
    # страницы самого чата. Это надёжнее, чем угадывать по тексту объявления.
    detail_html = _load_vkchats_card_detail(card, request_budget)
    if detail_html:
        exact = _extract_vkchats_detail_title(detail_html)
        if exact:
            card["title"] = exact
            return exact

    return ""


def _find_vkchats_title_near(page_html: str, position: int) -> str:
    # Берём небольшой кусок около конкретной ссылки, чтобы не схватить
    # название соседней карточки.
    left = max(0, int(position) - 1800)
    right = min(len(page_html), int(position) + 4200)
    chunk = page_html[left:right]

    candidates: list[tuple[int, str]] = []
    local_position = int(position) - left

    for priority, pattern in enumerate(_FALLBACK_TITLE_PATTERNS):
        for match in pattern.finditer(chunk):
            title = _decode_vkchats_title(match.group(1))
            if not title:
                continue

            distance = abs(match.start() - local_position)
            # owner_group/title важнее обычного title.
            score = distance + priority * 1000
            candidates.append((score, title))

    if not candidates:
        return ""

    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _parse_vkchats_fallback_cards(page_html: str) -> list[dict[str, Any]]:
    """Собирает уникальные приглашения прямо из сырой страницы.

    Это fallback для страниц, которые сайт отдаёт через сериализованный
    JSON/Next-разметку, а не обычные /chat/... карточки.
    """
    raw = html.unescape(str(page_html or ""))
    cards: list[dict[str, Any]] = []
    local_seen: set[str] = set()

    matches = list(_VK_JOIN_RE.finditer(raw))
    for match in matches:
        # Берём небольшой хвост — нормализатор сам остановится на конце URL.
        candidate = raw[match.start(): match.start() + 1000]
        link = _normalize_vk_join_link(candidate)
        if not link or link in local_seen:
            continue

        local_seen.add(link)
        title = _find_vkchats_title_near(raw, match.start())
        detail_url = _find_vkchats_detail_url_near(raw, match.start())
        cards.append(
            {
                "title": title,
                "text": link,
                "detail_url": detail_url,
                "_fallback_members": None,
            }
        )

    return cards


def _vkchats_fetch(url: str) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; WorkChatFinder/1.0; +https://vk.com/)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(req, timeout=VKCHATS_HTTP_TIMEOUT) as response:
        raw = response.read(2_500_000)
        charset = "utf-8"
        try:
            charset = response.headers.get_content_charset() or "utf-8"
        except Exception:
            pass
    return raw.decode(charset, errors="replace")


def _parse_vkchats_search_page(page_html: str) -> list[dict[str, Any]]:
    parser = _VkChatsSearchParser()
    try:
        parser.feed(page_html)
        parser.close()
    except Exception:
        logger.exception("Ошибка HTMLParser при разборе vkchats.ru")
    return parser.cards


def _load_chat_search_state() -> dict[str, Any]:
    with chat_search_state_lock:
        raw = read_json(
            CHAT_SEARCH_STATE_PATH,
            {"seen": {}, "query_cursor": 0, "query_pages": {}, "runs": 0},
        )
        if not isinstance(raw, dict):
            raw = {}

        old_seen = raw.get("seen")
        if not isinstance(old_seen, dict):
            old_seen = {}

        # Старые "грязные" варианты ссылки схлопываются в один чистый URL.
        seen: dict[str, Any] = {}
        seen_by_key: dict[str, str] = {}
        seen_changed = False

        for raw_link, meta in old_seen.items():
            clean_link = _normalize_vk_join_link(str(raw_link or ""))
            key = _vk_join_dedupe_key(clean_link)
            if not clean_link or not key:
                seen_changed = True
                continue

            if clean_link != str(raw_link):
                seen_changed = True

            previous_link = seen_by_key.get(key)
            if previous_link:
                # Уже есть тот же invite-токен — второй ключ удаляем.
                seen_changed = True
                # Предпочтём вариант с "=" в качестве сохранённого URL.
                if clean_link.endswith("=") and not previous_link.endswith("="):
                    previous_meta = seen.pop(previous_link, {})
                    seen[clean_link] = meta if isinstance(meta, dict) else previous_meta
                    seen_by_key[key] = clean_link
                continue

            seen[clean_link] = meta if isinstance(meta, dict) else {}
            seen_by_key[key] = clean_link

        if len(seen) != len(old_seen):
            seen_changed = True

        query_pages_raw = raw.get("query_pages")
        query_pages: dict[str, int] = {}
        if isinstance(query_pages_raw, dict):
            for query, page in query_pages_raw.items():
                try:
                    query_pages[str(query)] = max(1, int(page or 1))
                except (TypeError, ValueError):
                    pass

        try:
            cursor = max(0, int(raw.get("query_cursor", 0) or 0))
        except (TypeError, ValueError):
            cursor = 0
        try:
            runs = max(0, int(raw.get("runs", 0) or 0))
        except (TypeError, ValueError):
            runs = 0

        active_search_raw = raw.get("active_search")
        active_search = (
            dict(active_search_raw)
            if isinstance(active_search_raw, dict) and active_search_raw.get("active")
            else {}
        )

        state = {
            "seen": seen,
            "query_cursor": cursor,
            "query_pages": query_pages,
            "runs": runs,
            "query_strategy": str(raw.get("query_strategy") or ""),
            "active_search": active_search,
        }
        if seen_changed:
            write_json_atomic(CHAT_SEARCH_STATE_PATH, state)
        return state


def _save_chat_search_state(state: dict[str, Any]) -> None:
    with chat_search_state_lock:
        write_json_atomic(CHAT_SEARCH_STATE_PATH, state)


def _set_chat_search_active(
    active: bool,
    *,
    chat_id: int = 0,
    requested_limit: int = VKCHATS_RESULT_LIMIT,
    exact_query: str | None = None,
    min_members: int = 0,
) -> None:
    """Помечает поиск активным, чтобы после аварийного рестарта продолжить его."""
    state = _load_chat_search_state()
    if active:
        state["active_search"] = {
            "active": True,
            "chat_id": int(chat_id),
            "requested_limit": int(requested_limit),
            "exact_query": " ".join(str(exact_query or "").split()).strip() or None,
            "min_members": max(0, int(min_members or 0)),
            "started_at": now_msk_str(),
        }
    else:
        state["active_search"] = {}
    _save_chat_search_state(state)



def _fetch_json_url(url: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; WorkChatFinder/1.1; +https://vk.com/)",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(req, timeout=max(8.0, VKCHATS_HTTP_TIMEOUT)) as response:
        raw = response.read(12_000_000)
        charset = "utf-8"
        try:
            charset = response.headers.get_content_charset() or "utf-8"
        except Exception:
            pass
    return json.loads(raw.decode(charset, errors="replace"))


def _clean_geo_term(value: Any) -> str:
    return " ".join(str(value or "").replace("ё", "ё").split()).strip()


def _initials(value: str) -> str:
    words = re.findall(r"[А-ЯЁA-Z][а-яёa-z-]*", str(value or ""))
    if len(words) < 2:
        return ""
    return "".join(word[0].upper() for word in words if word)


def _region_stem(name: str) -> str:
    value = _clean_geo_term(name)
    lower = value.lower()
    suffixes = (
        "ская", "цкая", "жская", "шская",
        "ский", "цкий", "жский", "шский",
        "ское", "цкое",
    )
    for suffix in suffixes:
        if lower.endswith(suffix) and len(value) > len(suffix) + 2:
            return value[: -len(suffix)]
    return value


_RF_SPECIAL_REGION_ALIASES = {
    "Московская область": ["МО", "Моск обл", "Моск. обл.", "Московск обл", "Подмосковье"],
    "Ленинградская область": ["ЛО", "Лен обл", "Лен. обл.", "Ленобласть"],
    "Новосибирская область": ["НСО", "Новосибир обл", "Новосиб обл"],
    "Свердловская область": ["СО", "Свердлов обл", "Свердл обл"],
    "Нижегородская область": ["НО", "Нижегород обл", "Нижегор обл"],
    "Ростовская область": ["РО", "Ростов обл"],
    "Челябинская область": ["ЧО", "Челябин обл"],
    "Самарская область": ["Самар обл"],
    "Омская область": ["Омск обл"],
    "Воронежская область": ["Воронеж обл"],
    "Волгоградская область": ["Волгоград обл"],
    "Саратовская область": ["Саратов обл"],
    "Тюменская область": ["Тюмен обл"],
    "Иркутская область": ["Иркутск обл"],
    "Калужская область": ["Калуж обл"],
    "Тульская область": ["Тул обл"],
    "Рязанская область": ["Рязан обл"],
    "Ярославская область": ["Ярослав обл"],
    "Кемеровская область": ["Кузбасс", "Кемеров обл"],
    "Ханты-Мансийский автономный округ - Югра": ["ХМАО", "ХМАО-Югра", "Югра"],
    "Ханты-Мансийский автономный округ — Югра": ["ХМАО", "ХМАО-Югра", "Югра"],
    "Ямало-Ненецкий автономный округ": ["ЯНАО"],
    "Ненецкий автономный округ": ["НАО"],
    "Чукотский автономный округ": ["ЧАО"],
    "Еврейская автономная область": ["ЕАО"],
    "Санкт-Петербург": ["СПб", "СПБ", "Питер"],
    "Москва": ["МСК"],
    "Республика Башкортостан": ["РБ", "Башкирия"],
    "Республика Татарстан": ["РТ", "Татарстан"],
    "Республика Дагестан": ["РД", "Дагестан"],
    "Республика Бурятия": ["РБур", "Бурятия"],
    "Республика Саха (Якутия)": ["РСЯ", "Якутия", "Саха"],
    "Краснодарский край": ["КК", "Краснодар край", "Кубань"],
    "Красноярский край": ["ККрай", "Краснояр край"],
    "Приморский край": ["ПК", "Приморье"],
    "Хабаровский край": ["ХК", "Хабаров край"],
    "Ставропольский край": ["СК", "Ставрополье"],
    "Пермский край": ["ПКрай", "Перм край"],
    "Алтайский край": ["АК", "Алтай край"],
    "Забайкальский край": ["ЗК", "Забайкалье"],
    "Камчатский край": ["Камчатка"],
}


def _region_search_aliases(region: dict[str, Any]) -> list[str]:
    aliases: list[str] = []

    name = _clean_geo_term(region.get("name"))
    fullname = _clean_geo_term(region.get("fullname"))
    region_type = _clean_geo_term(region.get("type"))
    type_short = _clean_geo_term(region.get("typeShort"))
    unofficial = _clean_geo_term(region.get("unofficialName"))
    name_en = _clean_geo_term(region.get("name_en"))
    code = _clean_geo_term(region.get("code"))

    for value in (fullname, name, unofficial, name_en):
        if value:
            aliases.append(value)

    # "Московская обл", "Московская обл.", "Краснодарский кр", "Респ Татарстан".
    if name and type_short:
        aliases.extend([
            f"{name} {type_short}",
            f"{name} {type_short}.",
        ])

    if region_type.lower() == "область" and name:
        stem = _region_stem(name)
        aliases.extend([
            f"{name} обл",
            f"{name} обл.",
            f"{stem} обл",
            f"{stem} обл.",
        ])
        # Ещё более короткая форма полезна в названиях VK-бесед: "Моск обл".
        if len(stem) >= 5:
            aliases.append(f"{stem[:4]} обл")

    elif region_type.lower() == "край" and name:
        stem = _region_stem(name)
        aliases.extend([
            f"{name} кр",
            f"{name} кр.",
            f"{stem} край",
        ])

    elif region_type.lower() == "республика" and name:
        aliases.extend([
            f"Респ {name}",
            f"Респ. {name}",
        ])

    # Автоматическая аббревиатура: "Московская область" -> "МО",
    # "Республика Татарстан" -> "РТ".
    if fullname:
        short = _initials(fullname)
        if 2 <= len(short) <= 6:
            aliases.append(short)

    # Код субъекта тоже иногда встречается в названиях рабочих бесед.
    if code:
        aliases.extend([f"{code} регион", f"регион {code}"])

    for key in (fullname, name):
        if key and key in _RF_SPECIAL_REGION_ALIASES:
            aliases.extend(_RF_SPECIAL_REGION_ALIASES[key])

    return list(dict.fromkeys(value for value in aliases if value))


def _read_rf_geo_cache() -> dict[str, Any] | None:
    try:
        if not RF_GEO_CACHE_PATH.exists():
            return None
        age = max(0.0, time.time() - RF_GEO_CACHE_PATH.stat().st_mtime)
        raw = read_json(RF_GEO_CACHE_PATH, {})
        if not isinstance(raw, dict):
            return None
        cities_ru = raw.get("cities_ru")
        regions = raw.get("region_aliases")
        if not isinstance(cities_ru, list) or not cities_ru:
            return None
        if not isinstance(regions, list):
            regions = []
        raw["_fresh"] = age <= RF_GEO_CACHE_MAX_AGE_SEC
        return raw
    except Exception:
        logger.exception("Не удалось прочитать rf_search_locations.json")
        return None


def _save_rf_geo_cache(data: dict[str, Any]) -> None:
    try:
        write_json_atomic(
            RF_GEO_CACHE_PATH,
            {
                "updated_at": now_msk_str(),
                "cities_ru": data.get("cities_ru", []),
                "cities_en": data.get("cities_en", []),
                "region_aliases": data.get("region_aliases", []),
            },
        )
    except Exception:
        logger.exception("Не удалось сохранить rf_search_locations.json")


def _load_rf_geo_search_data() -> dict[str, Any]:
    global _rf_geo_cache_memory

    with _rf_geo_cache_lock:
        if isinstance(_rf_geo_cache_memory, dict):
            return _rf_geo_cache_memory

        cached = _read_rf_geo_cache()
        if cached and cached.get("_fresh"):
            cached.pop("_fresh", None)
            _rf_geo_cache_memory = cached
            return cached

        cities_ru: list[str] = []
        cities_en: list[str] = []
        region_aliases: list[str] = []

        try:
            cities_data = _fetch_json_url(RF_CITIES_DATA_URL)
            regions_data = _fetch_json_url(RF_REGIONS_DATA_URL)

            if isinstance(cities_data, list):
                for item in cities_data:
                    if not isinstance(item, dict):
                        continue
                    for key in ("name", "name_alt"):
                        value = _clean_geo_term(item.get(key))
                        if value:
                            cities_ru.append(value)
                    value_en = _clean_geo_term(item.get("name_en"))
                    if value_en:
                        cities_en.append(value_en)

            if isinstance(regions_data, list):
                for region in regions_data:
                    if isinstance(region, dict):
                        region_aliases.extend(_region_search_aliases(region))

            result = {
                "cities_ru": list(dict.fromkeys(cities_ru)),
                "cities_en": list(dict.fromkeys(cities_en)),
                "region_aliases": list(dict.fromkeys(region_aliases)),
            }

            if result["cities_ru"]:
                _save_rf_geo_cache(result)
                _rf_geo_cache_memory = result
                logger.info(
                    "Загружен справочник поиска: %s городов, %s английских названий, %s вариантов регионов",
                    len(result["cities_ru"]),
                    len(result["cities_en"]),
                    len(result["region_aliases"]),
                )
                return result

        except Exception as exc:
            logger.warning("Не удалось обновить полный справочник городов РФ: %s", exc)

        # Если обновление не получилось, используем старый кэш независимо от возраста.
        if cached:
            cached.pop("_fresh", None)
            _rf_geo_cache_memory = cached
            return cached

        # Последний fallback — встроенный список, чтобы /поискчаты не ломался.
        result = {
            "cities_ru": list(dict.fromkeys(VKCHATS_CITIES)),
            "cities_en": [],
            "region_aliases": [],
        }
        _rf_geo_cache_memory = result
        return result


def _build_vkchats_queries(
    exact_query: str | None = None,
    *,
    run_number: int = 0,
) -> list[str]:
    exact_query = " ".join(str(exact_query or "").split()).strip()
    if exact_query:
        # Ручной запрос ищется как написан, но бот листает ВСЕ его страницы.
        return [exact_query]

    geo = _load_rf_geo_search_data()
    queries: list[str] = []

    # Каждый законченный запуск переключает профиль. Если запуск остановили,
    # runs не увеличивается, поэтому продолжение остаётся на том же профиле.
    groups = VKCHATS_DISCOVERY_GROUPS or []
    if groups:
        group_index = max(0, int(run_number or 0)) % len(groups)
        active_group = groups[group_index]
    else:
        group_index = 0
        active_group = {"name": "базовый", "queries": [], "city_templates": [], "region_templates": []}

    discovery_queries = [
        " ".join(str(value).split()).strip()
        for value in active_group.get("queries", [])
        if str(value).strip()
    ]
    city_templates = [
        " ".join(str(value).split()).strip()
        for value in active_group.get("city_templates", [])
        if "{geo}" in str(value)
    ]
    region_templates = [
        " ".join(str(value).split()).strip()
        for value in active_group.get("region_templates", [])
        if "{geo}" in str(value)
    ]

    # 1) Сначала новый профиль — это даёт свежие результаты в начале запуска,
    # а не после тысяч уже выработанных «Работа <город>» запросов.
    queries.extend(discovery_queries)
    for phrase in discovery_queries:
        # Не плодим бессмысленные длинные фразы, но пробуем три сильных
        # контекста для коротких названий профессий/сфер.
        if len(phrase.split()) <= 3:
            queries.append(f"Работа {phrase}")
            queries.append(f"Подработка {phrase}")
            queries.append(f"Вакансии {phrase}")

    # 2) Базовые широкие запросы оставляем на каждом запуске: по ним могут
    # появиться НОВЫЕ беседы с момента предыдущего прохода.
    core = [
        "Работа", "Подработка", "Вакансии", "Рабочие", "Работники",
        "Сотрудники", "Вахта", "Калым", "Шабашка", "Заработок",
        "Ищу работу", "Ищу подработку", "Есть работа", "Нужны работники",
        "Требуются работники", "Jobs", "Work", "Hiring",
    ]
    queries.extend(core)

    # Несколько общих комбинаций вместо полного декартова взрыва.
    pair_phrases = [
        "Работа Подработка", "Работа Вакансии", "Работа Рабочие",
        "Работа Работники", "Работа Сотрудники", "Работа Вахта",
        "Подработка Вакансии", "Работа Склад", "Работа Производство",
        "Работа Стройка", "Работа Доставка", "Работа Логистика",
        "Работа Разнорабочие", "Работа Грузчики", "Работа Курьеры",
        "Вакансии Склад", "Вакансии Производство", "Вакансии Стройка",
    ]
    queries.extend(pair_phrases)

    # 3) Остальные одиночные формулировки из старой версии тоже сохраняем.
    queries.extend(VKCHATS_WORK_KEYWORDS)

    cities_ru = list(dict.fromkeys(
        [*VKCHATS_CITIES, *[str(v) for v in geo.get("cities_ru", []) if str(v).strip()]]
    ))
    cities_en = list(dict.fromkeys(
        [str(v) for v in geo.get("cities_en", []) if str(v).strip()]
    ))
    region_aliases = list(dict.fromkeys(
        [str(v) for v in geo.get("region_aliases", []) if str(v).strip()]
    ))

    # 4) Сначала приоритетные города из встроенного списка + АКТИВНЫЙ профиль.
    # Это самый «урожайный» блок, поэтому он расположен раньше полного справочника.
    priority_cities = list(dict.fromkeys(str(v).strip() for v in VKCHATS_CITIES if str(v).strip()))
    for city in priority_cities:
        for template in city_templates:
            queries.append(template.format(geo=city))

    # 5) Полный справочник городов. Базовых шаблонов стало меньше, чтобы
    # поиск не тонул в десятках тысяч почти одинаковых комбинаций.
    city_keywords_ru = (
        "Работа", "Подработка", "Вакансии", "Рабочие", "Вахта", "Нужны сотрудники",
    )
    for city in cities_ru:
        for keyword in city_keywords_ru:
            queries.append(f"{keyword} {city}")

        # Для КАЖДОГО города добавляем до 3 запросов активного профиля.
        # На следующем полном запуске профиль сменится и будут другие формулировки.
        for template in city_templates[:3]:
            queries.append(template.format(geo=city))

    # 6) Регионы и их сокращения: МО, ЛО, ХМАО, Подмосковье и т.д.
    region_keywords = ("Работа", "Подработка", "Вакансии", "Вахта")
    for region in region_aliases:
        for keyword in region_keywords:
            queries.append(f"{keyword} {region}")
        for template in region_templates[:3]:
            queries.append(template.format(geo=region))

    # 7) Английские названия городов встречаются реже, поэтому только два
    # базовых варианта — этого достаточно, чтобы не раздувать очередь.
    for city in cities_en:
        queries.append(f"Jobs {city}")
        queries.append(f"Work {city}")

    # Нормализация + dedupe с сохранением порядка.
    return list(dict.fromkeys(
        " ".join(str(query).split()).strip()
        for query in queries
        if str(query).strip()
    ))



def _extract_vkchats_last_page_hint(page_html: str) -> int | None:
    """Пытается узнать последнюю страницу из пагинации vkchats.ru."""
    raw = html.unescape(str(page_html or ""))
    pages: list[int] = []

    for match in re.finditer(r"/search/(\d+)(?:[?\"#<>\s]|$)", raw, flags=re.IGNORECASE):
        try:
            value = int(match.group(1))
        except (TypeError, ValueError):
            continue
        if value > 0:
            pages.append(value)

    for match in re.finditer(r"\\/search\\/(\d+)", raw, flags=re.IGNORECASE):
        try:
            value = int(match.group(1))
        except (TypeError, ValueError):
            continue
        if value > 0:
            pages.append(value)

    return max(pages) if pages else None


def _merge_vkchats_search_cards(page_html: str) -> list[dict[str, Any]]:
    """Возвращает именно карточки текущей страницы поиска.

    Раньше сюда ВСЕГДА примешивался fallback, который сканировал весь сырой
    HTML/Next.js payload. Из-за этого одна страница могла внезапно содержать
    сотни служебных vk.me/join ссылок, не относящихся к видимой выдаче.

    Теперь обычные /chat/... карточки — основной и единственный источник.
    Fallback включается только если сайт реально перестал отдавать карточки.
    """
    primary = _parse_vkchats_search_page(page_html)
    # Fail closed: если сайт изменил разметку и обычные карточки не
    # распарсились, НЕ сканируем весь Next.js HTML. Лучше получить 0 карточек
    # и запись в логах, чем сотни посторонних invite-ссылок.
    source = primary

    merged: list[dict[str, Any]] = []
    fingerprints: set[str] = set()

    for card in source:
        if not isinstance(card, dict):
            continue

        detail_url = str(card.get("detail_url") or "").strip()
        title = " ".join(str(card.get("title") or "").split()).strip()
        card_text = str(card.get("text") or "")

        direct_links = _extract_vk_join_links(card_text)
        normalized_links = sorted({
            _normalize_vk_join_link(link)
            for link in direct_links
            if _normalize_vk_join_link(link)
        })

        fingerprint = (
            detail_url
            + "|"
            + "|".join(normalized_links)
            + "|"
            + title
            + "|"
            + card_text[:300]
        )
        if fingerprint in fingerprints:
            continue

        fingerprints.add(fingerprint)
        merged.append(card)

    return merged


def _extract_all_direct_vkchats_page_items(
    page_html: str,
    *,
    query: str,
    min_members: int = 0,
) -> list[dict[str, Any]]:
    """Независимо вытаскивает все прямые vk.me/join ссылки из сырого HTML."""
    raw = html.unescape(str(page_html or ""))
    page_members = _extract_vkchats_member_count(raw)

    if min_members > 0 and isinstance(page_members, int) and page_members < min_members:
        return []

    items: list[dict[str, Any]] = []
    local_seen: set[str] = set()

    for match in _VK_JOIN_RE.finditer(raw):
        candidate = raw[match.start(): match.start() + 1200]
        link = _normalize_vk_join_link(candidate)
        key = _vk_join_dedupe_key(link)

        if not link or not key or key in local_seen:
            continue

        local_seen.add(key)
        title = _find_vkchats_title_near(raw, match.start()) or "Чат"

        items.append({
            "title": title,
            "link": link,
            "query": query,
            "members": page_members if isinstance(page_members, int) else None,
        })

    return items


def _vkchats_search_url(query: str, page: int) -> str:
    return f"{VKCHATS_BASE_URL}/search/{max(1, int(page))}?query={quote(query)}"


def _load_vkchats_card_detail(card: dict[str, Any], request_budget: list[int]) -> str:
    cached = card.get("_detail_html")
    if isinstance(cached, str):
        return cached

    detail_url = str(card.get("detail_url") or "").strip()
    if not detail_url:
        return ""

    try:
        detail_html = _vkchats_fetch(detail_url)
        request_budget[0] += 1
        card["_detail_html"] = detail_html
        time.sleep(VKCHATS_REQUEST_PAUSE)
        return detail_html
    except Exception as exc:
        request_budget[0] += 1
        card["_detail_html"] = ""
        logger.debug("Не удалось открыть карточку vkchats %s: %s", detail_url, exc)
        return ""


def _resolve_vkchats_card_links(card: dict[str, Any], request_budget: list[int]) -> list[str]:
    direct = _extract_vk_join_links(str(card.get("text") or ""))
    if direct:
        return direct

    # На части карточек приглашение видно только на отдельной странице чата.
    detail_html = _load_vkchats_card_detail(card, request_budget)
    return _extract_vk_join_links(detail_html)


def _resolve_vkchats_card_members(card: dict[str, Any], request_budget: list[int]) -> int | None:
    # Сначала берём число прямо из карточки поиска: сайт обычно пишет
    # "Участников: 123".
    members = _extract_vkchats_member_count(str(card.get("text") or ""))
    if members is not None:
        return members

    # Если в выдаче число не попало в карточку, читаем страницу самого чата.
    detail_html = _load_vkchats_card_detail(card, request_budget)
    if not detail_html:
        return None
    return _extract_vkchats_member_count(detail_html)



def _resolve_vkchats_card_fast(
    card: dict[str, Any],
    request_budget: list[int],
    min_members: int,
    page_html: str,
) -> list[dict[str, Any]]:
    """Разбирает карточку и ОБЯЗАТЕЛЬНО проверяет её detail-описание.

    Полезный сценарий vkchats.ru: в самой карточке может быть основной чат,
    а в описании внутри карточки — ещё несколько рабочих vk.me/join ссылок.
    Их нельзя терять. При этом detail разбирается только как видимый HTML:
    служебный Next.js/JSON payload не участвует.
    """
    card_text = str(card.get("text") or "")

    members = card.get("_fallback_members")
    if not isinstance(members, int):
        members = _extract_vkchats_member_count(card_text)

    title = " ".join(str(card.get("title") or "").split()).strip()
    if title.lower() in {"без названия", "чат", "открыть чат", ">"}:
        title = ""

    # 1) Прямые ссылки, которые реально видны в поисковой карточке.
    search_links = _extract_vk_join_links(card_text)

    # 2) Даже если основная ссылка уже есть в выдаче, всё равно открываем
    # detail: пользовательские описания часто содержат дополнительные чаты.
    detail_html = _load_vkchats_card_detail(card, request_budget)
    detail_links = _extract_vkchats_visible_detail_links(detail_html) if detail_html else []

    if detail_html:
        if min_members > 0 and not isinstance(members, int):
            members = _extract_vkchats_member_count(detail_html)
        if not title:
            title = _extract_vkchats_detail_title(detail_html)

    if min_members > 0 and (members is None or members < min_members):
        return []

    # Объединяем оба источника по invite-token, сохраняя порядок:
    # сначала основная карточка, затем дополнительные ссылки из описания.
    combined: list[tuple[str, str]] = []
    local_keys: set[str] = set()

    for source, links in (("search_card", search_links), ("detail_description", detail_links)):
        for raw_link in links:
            normalized = _normalize_vk_join_link(raw_link)
            key = _vk_join_dedupe_key(normalized)
            if not normalized or not key or key in local_keys:
                continue
            local_keys.add(key)
            combined.append((normalized, source))

    if not combined:
        return []

    items: list[dict[str, Any]] = []
    for normalized, source in combined:
        resolved_title = title
        if not resolved_title:
            try:
                position = page_html.find(normalized)
                if position >= 0:
                    resolved_title = _find_vkchats_title_near(page_html, position)
            except Exception:
                resolved_title = ""

        if not resolved_title:
            resolved_title = "Чат"

        items.append(
            {
                "title": resolved_title,
                "link": normalized,
                "members": members,
                "source": source,
            }
        )

    return items


def _render_chat_search_batch(items: list[dict[str, Any]], start_index: int) -> str:
    blocks: list[str] = []

    for idx, item in enumerate(items, start=start_index):
        title = " ".join(str(item.get("title") or "").split()).strip()
        if len(title) > 150:
            title = title[:147].rstrip() + "..."

        members = item.get("members")
        link = _normalize_vk_join_link(str(item.get("link") or ""))
        if not link:
            continue

        # Если название реально удалось достать:
        if title and title.lower() != "без названия":
            first_line = f"{idx}. {title}"
        else:
            # Не засоряем выдачу сотней одинаковых "Без названия".
            first_line = f"{idx}. Чат"

        lines = [first_line]

        if isinstance(members, int):
            lines.append(f"👥 {members}")

        # Ссылка отдельной строкой без "Ссылка:", чтобы никакой текст
        # визуально не слипался с URL.
        lines.append(link)

        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)



FOUND_CHATS_PAGE_SIZE = 12


def _load_found_chats_database() -> dict[str, Any]:
    with found_chats_db_lock:
        data = read_json(FOUND_CHATS_DB_PATH, {"items": {}})
        if not isinstance(data, dict):
            data = {"items": {}}
        if not isinstance(data.get("items"), dict):
            data["items"] = {}
        return data


def _save_found_chats_database(data: dict[str, Any]) -> None:
    with found_chats_db_lock:
        write_json_atomic(FOUND_CHATS_DB_PATH, data)


def _store_found_chat(
    link: str,
    *,
    title: str = "",
    query: str = "",
    members: int | None = None,
) -> None:
    """Навсегда сохраняет найденный чат в отдельную базу."""
    clean_link = _normalize_vk_join_link(str(link or ""))
    key = _vk_join_dedupe_key(clean_link)
    if not clean_link or not key:
        return

    now_value = now_msk_str()

    with found_chats_db_lock:
        data = read_json(FOUND_CHATS_DB_PATH, {"items": {}})
        if not isinstance(data, dict):
            data = {"items": {}}
        items = data.get("items")
        if not isinstance(items, dict):
            items = {}
            data["items"] = items

        old = items.get(key)
        if not isinstance(old, dict):
            old = {}

        clean_title = " ".join(str(title or "").split()).strip()
        clean_query = " ".join(str(query or "").split()).strip()

        items[key] = {
            "link": clean_link,
            "title": clean_title or str(old.get("title") or ""),
            "query": clean_query or str(old.get("query") or ""),
            "members": members if isinstance(members, int) else old.get("members"),
            "first_found_at": str(old.get("first_found_at") or now_value),
            "last_found_at": now_value,
        }

        write_json_atomic(FOUND_CHATS_DB_PATH, data)


def _backfill_found_chats_database() -> int:
    """Один раз переносит уже найденные ранее чаты из chat_search_seen.json."""
    state = _load_chat_search_state()
    seen_map = state.get("seen") or {}
    if not isinstance(seen_map, dict):
        return 0

    added = 0
    with found_chats_db_lock:
        data = read_json(FOUND_CHATS_DB_PATH, {"items": {}})
        if not isinstance(data, dict):
            data = {"items": {}}
        items = data.get("items")
        if not isinstance(items, dict):
            items = {}
            data["items"] = items

        for raw_link, meta in seen_map.items():
            clean_link = _normalize_vk_join_link(str(raw_link or ""))
            key = _vk_join_dedupe_key(clean_link)
            if not clean_link or not key:
                continue

            if key in items:
                continue

            meta = meta if isinstance(meta, dict) else {}
            items[key] = {
                "link": clean_link,
                "title": str(meta.get("title") or ""),
                "query": str(meta.get("query") or ""),
                "members": meta.get("members"),
                "first_found_at": str(meta.get("first_sent_at") or now_msk_str()),
                "last_found_at": str(meta.get("first_sent_at") or now_msk_str()),
            }
            added += 1

        if added:
            write_json_atomic(FOUND_CHATS_DB_PATH, data)

    return added


def render_found_chats_database(page: int = 1) -> str:
    """Показывает страницу постоянной базы найденных чатов."""
    _backfill_found_chats_database()
    data = _load_found_chats_database()
    items_map = data.get("items") or {}
    if not isinstance(items_map, dict):
        items_map = {}

    items = [
        value
        for value in items_map.values()
        if isinstance(value, dict) and _normalize_vk_join_link(str(value.get("link") or ""))
    ]

    # Новые сверху.
    items.sort(
        key=lambda value: str(value.get("first_found_at") or ""),
        reverse=True,
    )

    total = len(items)
    if total == 0:
        return "📚 База найденных чатов пока пуста."

    total_pages = max(1, math.ceil(total / FOUND_CHATS_PAGE_SIZE))
    page = max(1, min(int(page or 1), total_pages))

    start = (page - 1) * FOUND_CHATS_PAGE_SIZE
    batch = items[start:start + FOUND_CHATS_PAGE_SIZE]

    lines = [
        "📚 БАЗА НАЙДЕННЫХ ЧАТОВ",
        "",
        f"Всего сохранено: {total}",
        f"Страница: {page}/{total_pages}",
        "",
    ]

    for index, item in enumerate(batch, start=start + 1):
        title = " ".join(str(item.get("title") or "").split()).strip() or "Чат"
        if len(title) > 100:
            title = title[:97].rstrip() + "..."

        link = _normalize_vk_join_link(str(item.get("link") or ""))
        members = item.get("members")
        query = " ".join(str(item.get("query") or "").split()).strip()

        lines.append(f"{index}. {title}")
        if isinstance(members, int):
            lines.append(f"👥 {members}")
        if query:
            lines.append(f"🔎 {query}")
        lines.append(link)
        lines.append("")

    if total_pages > 1:
        lines.append(
            f"Следующая страница: .базачаты {min(page + 1, total_pages)}"
            if page < total_pages
            else f"Первая страница: .базачаты 1"
        )

    return "\n".join(lines).strip()



ARCHIVE_CHATS_PAGE_SIZE = 10


def render_found_chats_archive(page: int = 1) -> str:
    """Полный архив всех чатов, когда-либо найденных ботом."""
    # Подтягиваем старую историю из chat_search_seen.json,
    # поэтому архив включает и находки до появления команды.
    _backfill_found_chats_database()

    data = _load_found_chats_database()
    items_map = data.get("items") or {}
    if not isinstance(items_map, dict):
        items_map = {}

    items: list[dict[str, Any]] = []
    for value in items_map.values():
        if not isinstance(value, dict):
            continue

        link = _normalize_vk_join_link(str(value.get("link") or ""))
        if not link:
            continue

        item = dict(value)
        item["link"] = link
        items.append(item)

    # Самые новые находки сверху.
    items.sort(
        key=lambda value: (
            str(value.get("first_found_at") or ""),
            str(value.get("title") or ""),
        ),
        reverse=True,
    )

    total = len(items)
    with_members = sum(
        1
        for item in items
        if isinstance(item.get("members"), int)
    )
    with_titles = sum(
        1
        for item in items
        if str(item.get("title") or "").strip()
        and str(item.get("title") or "").strip().lower() not in {"чат", "без названия"}
    )

    if total == 0:
        return "🗄 АРХИВ НАЙДЕННЫХ ЧАТОВ\n\nАрхив пока пуст."

    total_pages = max(
        1,
        math.ceil(total / ARCHIVE_CHATS_PAGE_SIZE),
    )
    safe_page = max(
        1,
        min(int(page or 1), total_pages),
    )

    start = (safe_page - 1) * ARCHIVE_CHATS_PAGE_SIZE
    batch = items[start:start + ARCHIVE_CHATS_PAGE_SIZE]

    lines = [
        "🗄 АРХИВ НАЙДЕННЫХ ЧАТОВ",
        "",
        f"Всего найдено за всё время: {total}",
        f"С известным названием: {with_titles}",
        f"С известным числом участников: {with_members}",
        f"Страница: {safe_page}/{total_pages}",
        "",
    ]

    for index, item in enumerate(batch, start=start + 1):
        title = " ".join(
            str(item.get("title") or "").split()
        ).strip() or "Название неизвестно"

        if title.lower() in {"чат", "без названия"}:
            title = "Название неизвестно"

        if len(title) > 110:
            title = title[:107].rstrip() + "..."

        members = item.get("members")
        members_text = (
            str(members)
            if isinstance(members, int)
            else "неизвестно"
        )

        query = " ".join(
            str(item.get("query") or "").split()
        ).strip() or "неизвестен"

        first_found = str(
            item.get("first_found_at") or ""
        ).strip() or "неизвестно"

        last_found = str(
            item.get("last_found_at") or ""
        ).strip() or first_found

        link = str(item.get("link") or "").strip()

        lines.extend([
            f"{index}. {title}",
            f"👥 Участники: {members_text}",
            f"🔎 Найден по запросу: {query}",
            f"🕐 Впервые найден: {first_found}",
        ])

        if last_found != first_found:
            lines.append(f"♻️ Последний раз найден: {last_found}")

        lines.append(link)
        lines.append("")

    if safe_page < total_pages:
        lines.append(
            f"Следующая страница: .архивчаты {safe_page + 1}"
        )
    elif total_pages > 1:
        lines.append("Первая страница: .архивчаты 1")

    return "\n".join(lines).strip()


def _parse_archive_chats_page(command_text: str) -> int:
    parts = str(command_text or "").strip().split()
    if len(parts) >= 2:
        try:
            return max(1, int(parts[1]))
        except (TypeError, ValueError):
            pass
    return 1



def _parse_found_chats_page(command_text: str) -> int:
    parts = str(command_text or "").strip().split()
    if len(parts) >= 2:
        try:
            return max(1, int(parts[1]))
        except (TypeError, ValueError):
            pass
    return 1



def _send_chat_search_results(
    chat_id: int,
    results: list[dict[str, Any]],
    query_counts: dict[str, int],
    state: dict[str, Any],
    *,
    start_index: int = 1,
    show_header: bool = True,
    show_empty: bool = True,
) -> int:
    """Отправляет найденные чаты сразу, не дожидаясь конца всего обхода."""
    results = _dedupe_chat_search_results(results)

    if not results:
        if show_empty:
            send_message(
                chat_id,
                "🔎 Поиск по чатам\n\nНовых чатов пока нет.",
                rate_limit=False,
            )
        return 0

    seen_map = state.setdefault("seen", {})
    sent_items = 0

    for offset in range(0, len(results), 10):
        batch = results[offset: offset + 10]
        body = _render_chat_search_batch(batch, start_index + offset)
        if not body:
            continue

        if show_header and offset == 0:
            body = "🔎 Найденные чаты\n\n" + body

        result = send_message(
            chat_id,
            body,
            rate_limit=False,
        )
        if result is None:
            # Не помечаем ссылки просмотренными, если сообщение реально не ушло.
            continue

        sent_items += len(batch)

        for item in batch:
            link = _normalize_vk_join_link(str(item.get("link") or ""))

            _store_found_chat(
                link,
                title=str(item.get("title") or ""),
                query=str(item.get("query") or ""),
                members=item.get("members") if isinstance(item.get("members"), int) else None,
            )

            key = _vk_join_dedupe_key(link)
            if not link or not key:
                continue

            for old_link in list(seen_map.keys()):
                if old_link != link and _vk_join_dedupe_key(old_link) == key:
                    seen_map.pop(old_link, None)

            seen_map[link] = {
                "title": str(item.get("title") or ""),
                "query": str(item.get("query") or ""),
                "members": item.get("members"),
                "first_sent_at": now_msk_str(),
            }

        _save_chat_search_state(state)
        time.sleep(0.12)

    return sent_items


def _run_vkchats_search(
    chat_id: int,
    requested_limit: int,
    exact_query: str | None = None,
    min_members: int = 0,
) -> None:
    """Полный поиск: один запрос полностью по всем страницам, затем следующий."""
    global chat_search_thread

    try:
        # Оставлено только для совместимости старого вызова.
        # Поиск больше НЕ прекращается после 200 найденных чатов.
        _ = requested_limit

        min_members = max(0, int(min_members or 0))
        exact_query = " ".join(str(exact_query or "").split()).strip() or None
        chat_search_cancel_event.clear()

        state = _load_chat_search_state()
        seen_map: dict[str, Any] = state.setdefault("seen", {})
        query_pages: dict[str, int] = state.setdefault("query_pages", {})

        # Один раз очищаем старый прогресс пагинации от параллельной схемы.
        # База уже найденных ссылок НЕ удаляется.
        strategy = "rotating_discovery_profiles_v6_card_plus_visible_detail"
        if state.get("query_strategy") != strategy:
            query_pages = {}
            state["query_pages"] = query_pages
            state["query_cursor"] = 0
            state["query_strategy"] = strategy
            _save_chat_search_state(state)

        queries = _build_vkchats_queries(exact_query, run_number=int(state.get("runs", 0) or 0))
        if not queries:
            send_message(
                chat_id,
                "❌ Не удалось сформировать поисковые запросы.",
                rate_limit=False,
            )
            return

        cursor = 0 if exact_query else int(state.get("query_cursor", 0) or 0) % len(queries)
        ordered_queries = queries[cursor:] + queries[:cursor]

        seen_keys: set[str] = {
            key
            for key in (_vk_join_dedupe_key(link) for link in seen_map.keys())
            if key
        }
        collected_keys: set[str] = set()

        total_found = 0
        total_sent = 0
        total_links_scanned = 0
        total_pages_checked = 0
        completed_queries = 0
        run_started = time.monotonic()
        request_budget = [0]

        with chat_search_runtime_lock:
            chat_search_runtime.update({
                "started_at": run_started,
                "pages_checked": 0,
                "queries_checked": 0,
                "found": 0,
                "sent": 0,
                "current_query": exact_query or "подготовка полного поиска",
            })

        if exact_query:
            send_message(
                chat_id,
                "🔎 Полный поиск запущен.\n"
                f"Запрос: {exact_query}\n\n"
                "Проверяю ВСЕ страницы этого запроса по порядку и ВСЕ ссылки "
                "на каждой странице. Лимита по страницам и новым чатам нет.",
                rate_limit=False,
            )
        else:
            profile_name = "базовый"
            if VKCHATS_DISCOVERY_GROUPS:
                profile_index = int(state.get("runs", 0) or 0) % len(VKCHATS_DISCOVERY_GROUPS)
                profile_name = str(VKCHATS_DISCOVERY_GROUPS[profile_index].get("name") or "базовый")
            send_message(
                chat_id,
                "🔎 Полный автопоиск запущен.\n"
                f"Профиль: {profile_name}.\n"
                f"Сформировано запросов: {len(queries)}.\n\n"
                "Схема теперь строгая:\n"
                "1 запрос → ВСЕ его страницы → ВСЕ ссылки на каждой странице → "
                "только потом следующий запрос.\n\n"
                "Лимита в 200 найденных чатов больше нет.",
                rate_limit=False,
            )

        # Строго ПО ОДНОМУ запросу.
        for relative_index, query in enumerate(ordered_queries):
            if chat_search_cancel_event.is_set():
                break

            query_index = (cursor + relative_index) % len(queries) if not exact_query else 0
            page = max(1, int(query_pages.get(query, 1) or 1))
            known_last_page: int | None = None
            seen_page_signatures: set[tuple[str, ...]] = set()

            pages_for_query = 0
            found_for_query = 0
            # Реальное количество УНИКАЛЬНЫХ invite-ссылок, встреченных по
            # этому запросу. Одна и та же ссылка из двух источников больше
            # не раздувает scanned.
            scanned_keys_for_query: set[str] = set()
            already_seen_keys_for_query: set[str] = set()
            already_found_this_run_keys: set[str] = set()
            source_search_keys_for_query: set[str] = set()
            source_detail_keys_for_query: set[str] = set()
            scanned_for_query = 0

            # ВСЕ страницы текущего запроса.
            while not chat_search_cancel_event.is_set():
                with chat_search_runtime_lock:
                    chat_search_runtime["current_query"] = (
                        f"{query} — страница {page}"
                        + (
                            f"/{known_last_page}"
                            if known_last_page is not None
                            else ""
                        )
                    )

                url = _vkchats_search_url(query, page)

                # Временная ошибка НЕ означает конец страниц.
                # Повторяем эту же страницу, пока не получим ответ.
                attempt = 0
                page_html: str | None = None

                while not chat_search_cancel_event.is_set() and page_html is None:
                    attempt += 1
                    try:
                        page_html = _vkchats_fetch(url)
                        request_budget[0] += 1
                    except Exception as exc:
                        request_budget[0] += 1
                        logger.warning(
                            "vkchats.ru: запрос=%r page=%s попытка=%s: %s",
                            query,
                            page,
                            attempt,
                            exc,
                        )
                        chat_search_cancel_event.wait(
                            min(5.0, 0.5 + attempt * 0.35)
                        )

                if chat_search_cancel_event.is_set():
                    break
                if page_html is None:
                    continue

                total_pages_checked += 1
                pages_for_query += 1

                page_hint = _extract_vkchats_last_page_hint(page_html)
                if page_hint is not None:
                    known_last_page = max(
                        int(known_last_page or 0),
                        int(page_hint),
                        page,
                    )

                # Берём только карточки текущей страницы поиска. Сканирование
                # ВСЕГО HTML удалено: Next.js payload содержит множество
                # посторонних/повторных приглашений и раньше раздувал scanned.
                cards = _merge_vkchats_search_cards(page_html)
                if not cards and ("/chat/" in page_html or "vk.me/join/" in page_html):
                    logger.warning(
                        "vkchats: страница %s запроса %r содержит chat/invite данные, "
                        "но видимые карточки не распарсились; сырой HTML намеренно не сканируется",
                        page,
                        query,
                    )

                page_results: list[dict[str, Any]] = []
                page_result_keys: set[str] = set()

                # Все карточки. Если invite есть только внутри detail,
                # detail открывается и ссылка тоже забирается.
                if cards:
                    with ThreadPoolExecutor(
                        max_workers=min(
                            VKCHATS_CARD_WORKERS,
                            max(1, len(cards)),
                        ),
                        thread_name_prefix="vkchats-card",
                    ) as card_pool:
                        futures = [
                            card_pool.submit(
                                _resolve_vkchats_card_fast,
                                card,
                                request_budget,
                                min_members,
                                page_html,
                            )
                            for card in cards
                        ]

                        for future in as_completed(futures):
                            if chat_search_cancel_event.is_set():
                                break

                            try:
                                resolved_items = future.result()
                            except Exception as exc:
                                logger.debug(
                                    "Ошибка карточки vkchats: %s",
                                    exc,
                                )
                                continue

                            for item in resolved_items:
                                normalized = _normalize_vk_join_link(
                                    str(item.get("link") or "")
                                )
                                key = _vk_join_dedupe_key(normalized)
                                if not normalized or not key:
                                    continue

                                source = str(item.get("source") or "")
                                if source == "detail_description":
                                    source_detail_keys_for_query.add(key)
                                elif source == "search_card":
                                    source_search_keys_for_query.add(key)

                                if key not in scanned_keys_for_query:
                                    scanned_keys_for_query.add(key)
                                    scanned_for_query = len(scanned_keys_for_query)
                                    total_links_scanned += 1

                                if key in seen_keys:
                                    already_seen_keys_for_query.add(key)
                                    continue
                                if key in collected_keys:
                                    already_found_this_run_keys.add(key)
                                    continue
                                if key in page_result_keys:
                                    continue

                                page_result_keys.add(key)
                                page_results.append({
                                    "title": str(item.get("title") or "Чат"),
                                    "link": normalized,
                                    "query": query,
                                    "members": item.get("members"),
                                })

                # Сначала сохраняем, потом отправляем.
                for item in page_results:
                    normalized = _normalize_vk_join_link(
                        str(item.get("link") or "")
                    )
                    key = _vk_join_dedupe_key(normalized)
                    if not normalized or not key:
                        continue

                    _store_found_chat(
                        normalized,
                        title=str(item.get("title") or "Чат"),
                        query=query,
                        members=item.get("members"),
                    )
                    collected_keys.add(key)

                if page_results:
                    found_for_query += len(page_results)
                    total_found += len(page_results)

                    sent = _send_chat_search_results(
                        chat_id,
                        page_results,
                        {query: len(page_results)},
                        state,
                        start_index=total_sent + 1,
                        show_header=(total_sent == 0),
                        show_empty=False,
                    )
                    total_sent += sent

                # Сохраняем следующую страницу после КАЖДОЙ успешной страницы.
                query_pages[query] = page + 1
                state["query_pages"] = query_pages
                state["query_strategy"] = strategy
                if not exact_query:
                    state["query_cursor"] = query_index
                _save_chat_search_state(state)

                # Не долбим сайт и хостинг без пауз: это снижает риск, что
                # процесс будет убит платформой из-за резкого CPU/HTTP всплеска.
                chat_search_cancel_event.wait(VKCHATS_REQUEST_PAUSE)

                with chat_search_runtime_lock:
                    chat_search_runtime.update({
                        "pages_checked": total_pages_checked,
                        "queries_checked": completed_queries,
                        "found": total_found,
                        "sent": total_sent,
                    })

                # Сигнатура нужна только как защита от ситуации, когда сайт
                # за пределами реальной пагинации повторяет последнюю страницу.
                card_signature = tuple(sorted(
                    str(card.get("detail_url") or "")
                    + "|"
                    + str(card.get("title") or "")
                    + "|"
                    + str(card.get("text") or "")[:300]
                    for card in cards
                ))
                full_signature = card_signature

                # Если сайт дал пагинацию, идём РОВНО до последней страницы.
                if known_last_page is not None:
                    if page >= known_last_page:
                        break
                else:
                    # Если пагинации нет, только реально пустая страница завершает запрос.
                    if not cards:
                        break

                    # Или сервер начал повторять уже пройденную страницу.
                    if full_signature and full_signature in seen_page_signatures:
                        break

                if full_signature:
                    seen_page_signatures.add(full_signature)

                page += 1

                if pages_for_query % 100 == 0:
                    last_text = (
                        str(known_last_page)
                        if known_last_page is not None
                        else "не указана сайтом"
                    )
                    send_message(
                        chat_id,
                        "⏳ Поиск продолжается.\n"
                        f"Запрос: {query}\n"
                        f"Страниц этого запроса проверено: {pages_for_query}\n"
                        f"Последняя страница по пагинации: {last_text}\n"
                        f"Ссылок просмотрено: {scanned_for_query}\n"
                        f"Новых чатов: {found_for_query}",
                        rate_limit=False,
                    )

            if chat_search_cancel_event.is_set():
                break

            # Только ЗДЕСЬ переходим к следующему запросу.
            completed_queries += 1
            query_pages[query] = 1

            if not exact_query:
                state["query_cursor"] = (query_index + 1) % len(queries)

            state["query_pages"] = query_pages
            state["query_strategy"] = strategy
            _save_chat_search_state(state)

            logger.info(
                "vkchats: запрос полностью пройден: %r pages=%s unique=%s from_cards=%s from_descriptions=%s already_seen=%s already_this_run=%s new=%s",
                query,
                pages_for_query,
                scanned_for_query,
                len(source_search_keys_for_query),
                len(source_detail_keys_for_query),
                len(already_seen_keys_for_query),
                len(already_found_this_run_keys),
                found_for_query,
            )

        # Профиль меняем только после ПОЛНОГО автоматического прохода.
        # Ручной запрос и остановленный/оборванный поиск не должны перескакивать
        # на следующий профиль.
        if (
            not exact_query
            and not chat_search_cancel_event.is_set()
            and completed_queries >= len(ordered_queries)
        ):
            state["runs"] = int(state.get("runs", 0) or 0) + 1

        state["query_pages"] = query_pages
        state["query_strategy"] = strategy
        _save_chat_search_state(state)

        elapsed = max(0, int(time.monotonic() - run_started))

        if chat_search_cancel_event.is_set():
            finish_text = (
                "⏹ Полный поиск остановлен.\n"
                f"Полностью пройдено запросов: {completed_queries}\n"
                f"Страниц проверено: {total_pages_checked}\n"
                f"Ссылок просмотрено: {total_links_scanned}\n"
                f"Новых чатов найдено: {total_found}\n"
                f"Отправлено: {total_sent}\n\n"
                "Текущая страница сохранена для продолжения."
            )
        else:
            finish_text = (
                "✅ Полный поиск завершён.\n"
                f"Полностью пройдено запросов: {completed_queries}\n"
                f"Страниц проверено: {total_pages_checked}\n"
                f"Ссылок просмотрено: {total_links_scanned}\n"
                f"Новых уникальных чатов найдено: {total_found}\n"
                f"Отправлено: {total_sent}\n"
                f"Время: {elapsed} сек."
            )

        send_message(
            chat_id,
            finish_text,
            rate_limit=False,
        )

    except Exception:
        logger.exception("Критическая ошибка /поискчаты")
        send_message(
            chat_id,
            "❌ Поиск чатов завершился с ошибкой. "
            "Уже найденные ссылки и последняя страница сохранены.",
            rate_limit=False,
        )
    finally:
        # Если процесс был убит извне, finally не выполнится и active_search
        # останется True — на следующем старте бот автоматически продолжит.
        # При обычном завершении/ручной остановке маркер очищаем.
        try:
            _set_chat_search_active(False)
        except Exception:
            logger.exception("Не удалось очистить active_search")

        chat_search_cancel_event.clear()
        with chat_search_runtime_lock:
            chat_search_runtime["current_query"] = ""
        with chat_search_lock:
            chat_search_thread = None


def _parse_chat_search_command(text_value: str) -> tuple[int, str | None, int]:
    """Разбирает /поискчаты [запрос] [минимум участников]."""
    raw = " ".join(str(text_value or "").split()).strip()
    parts = raw.split()
    args = parts[1:] if len(parts) > 1 else []

    # Лимит результата больше не используется; оставлен для совместимости.
    limit = VKCHATS_RESULT_LIMIT
    min_members = 0

    # Число в конце — минимальное количество участников:
    # /поискчаты 80
    # /поискчаты Работа Новосибирск 80
    if args and args[-1].isdigit():
        min_members = max(0, int(args[-1]))
        args = args[:-1]

    exact_query = " ".join(args).strip() or None
    return limit, exact_query, min_members



def start_vkchats_search(
    chat_id: int,
    requested_limit: int = 100,
    exact_query: str | None = None,
    min_members: int = 0,
) -> bool:
    global chat_search_thread

    with chat_search_lock:
        if chat_search_thread is not None and chat_search_thread.is_alive():
            return False

        chat_search_cancel_event.clear()
        _set_chat_search_active(
            True,
            chat_id=int(chat_id),
            requested_limit=int(requested_limit),
            exact_query=exact_query,
            min_members=int(min_members),
        )
        chat_search_thread = threading.Thread(
            target=_run_vkchats_search,
            args=(int(chat_id), int(requested_limit), exact_query, int(min_members)),
            name="vkchats-search",
            daemon=True,
        )
        chat_search_thread.start()
        return True


def _resume_interrupted_vkchats_search() -> bool:
    """Возобновляет поиск, если прошлый процесс был оборван хостингом."""
    try:
        state = _load_chat_search_state()
        info = state.get("active_search")
        if not isinstance(info, dict) or not info.get("active"):
            return False

        chat_id = int(info.get("chat_id") or 0)
        if not chat_id:
            _set_chat_search_active(False)
            return False

        requested_limit = int(info.get("requested_limit") or VKCHATS_RESULT_LIMIT)
        exact_query_raw = info.get("exact_query")
        exact_query = str(exact_query_raw).strip() if exact_query_raw else None
        min_members = max(0, int(info.get("min_members") or 0))

        logger.warning(
            "Найден оборванный /поискчаты: автоматически продолжаю "
            "chat_id=%s query=%r min_members=%s",
            chat_id, exact_query, min_members,
        )
        return start_vkchats_search(
            chat_id,
            requested_limit,
            exact_query,
            min_members,
        )
    except Exception:
        logger.exception("Не удалось автоматически продолжить /поискчаты")
        return False


def stop_vkchats_search() -> bool:
    with chat_search_lock:
        running = chat_search_thread is not None and chat_search_thread.is_alive()
        if running:
            chat_search_cancel_event.set()
        return running


def render_vkchats_search_status() -> str:
    with chat_search_runtime_lock:
        started_at = float(chat_search_runtime.get("started_at") or 0)
        pages = int(chat_search_runtime.get("pages_checked") or 0)
        queries_count = int(chat_search_runtime.get("queries_checked") or 0)
        found = int(chat_search_runtime.get("found") or 0)
        sent = int(chat_search_runtime.get("sent") or 0)
        current_query = str(chat_search_runtime.get("current_query") or "").strip()

    elapsed = max(0, int(time.monotonic() - started_at)) if started_at else 0
    return (
        "🔎 Поиск чатов уже работает.\n"
        f"Идёт: {elapsed} сек.\n"
        f"Проверено страниц: {pages}\n"
        f"Проверено запросов: {queries_count}\n"
        f"Найдено новых: {found}\n"
        f"Отправлено: {sent}"
        + (f"\nСейчас: {current_query}" if current_query else "")
        + "\n\nЧтобы остановить: /стоппоиск"
    )


def get_bulk_vk() -> Any:
    """Отдельный VK API-клиент на каждый массовый поток.

    Long Poll и интерактивные кнопки остаются на основном vk_session, поэтому
    массовая рассылка не забивает его HTTP-соединения.
    """
    api = getattr(_bulk_thread_local, "api", None)
    if api is None:
        session = vk_api.VkApi(token=group_token)
        api = session.get_api()
        _bulk_thread_local.session = session
        _bulk_thread_local.api = api
    return api


def wait_bulk_send_slot() -> None:
    """Общий мягкий лимитер только для массовых ЛС.

    Он никак не блокирует ответы на команды и callback-кнопки.
    """
    global _bulk_next_send_at
    with _bulk_send_lock:
        now = time.monotonic()
        wait_for = _bulk_next_send_at - now
        if wait_for > 0:
            time.sleep(wait_for)
        _bulk_next_send_at = time.monotonic() + BULK_SEND_INTERVAL_SEC

# Словарь покрывает крупнейшие города РФ и распространённые сокращения.
# Для падежей дополнительно используется поиск по устойчивым основам слов.
CITY_ALIASES: dict[str, str] = {
    "мск": "Москва", "москва": "Москва", "москве": "Москва", "москву": "Москва", "московский": "Москва",
    "спб": "Санкт-Петербург", "питер": "Санкт-Петербург", "петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург",
    "екб": "Екатеринбург", "екатеринбург": "Екатеринбург", "екатеринбурге": "Екатеринбург",
    "нск": "Новосибирск", "новосибирск": "Новосибирск", "новосибирске": "Новосибирск",
    "нн": "Нижний Новгород", "нижний новгород": "Нижний Новгород", "нижнем новгороде": "Нижний Новгород",
    "казань": "Казань", "казани": "Казань", "самара": "Самара", "самаре": "Самара",
    "омск": "Омск", "омске": "Омск", "уфа": "Уфа", "уфе": "Уфа",
    "челябинск": "Челябинск", "челябинске": "Челябинск", "ростов на дону": "Ростов-на-Дону", "ростов": "Ростов-на-Дону",
    "краснодар": "Краснодар", "краснодаре": "Краснодар", "пермь": "Пермь", "перми": "Пермь",
    "воронеж": "Воронеж", "воронеже": "Воронеж", "волгоград": "Волгоград", "волгограде": "Волгоград",
    "красноярск": "Красноярск", "красноярске": "Красноярск", "саратов": "Саратов", "саратове": "Саратов",
    "тюмень": "Тюмень", "тюмени": "Тюмень", "тольятти": "Тольятти", "ижевск": "Ижевск", "ижевске": "Ижевск",
    "барнаул": "Барнаул", "барнауле": "Барнаул", "ульяновск": "Ульяновск", "ульяновске": "Ульяновск",
    "иркутск": "Иркутск", "иркутске": "Иркутск", "хабаровск": "Хабаровск", "хабаровске": "Хабаровск",
    "ярославль": "Ярославль", "ярославле": "Ярославль", "владивосток": "Владивосток", "владивостоке": "Владивосток",
    "махачкала": "Махачкала", "махачкале": "Махачкала", "томск": "Томск", "томске": "Томск",
    "оренбург": "Оренбург", "оренбурге": "Оренбург", "кемерово": "Кемерово", "новокузнецк": "Новокузнецк",
    "рязань": "Рязань", "рязани": "Рязань", "астрахань": "Астрахань", "астрахани": "Астрахань",
    "набережные челны": "Набережные Челны", "пенза": "Пенза", "пензе": "Пенза", "липецк": "Липецк", "липецке": "Липецк",
    "киров": "Киров", "кирове": "Киров", "чебоксары": "Чебоксары", "тула": "Тула", "туле": "Тула",
    "калининград": "Калининград", "калининграде": "Калининград", "балашиха": "Балашиха", "курск": "Курск", "курске": "Курск",
    "улан удэ": "Улан-Удэ", "ставрополь": "Ставрополь", "ставрополе": "Ставрополь", "сочи": "Сочи",
    "тверь": "Тверь", "твери": "Тверь", "магнитогорск": "Магнитогорск", "иваново": "Иваново",
    "брянск": "Брянск", "брянске": "Брянск", "белгород": "Белгород", "белгороде": "Белгород",
    "сургут": "Сургут", "сургуте": "Сургут", "владимир": "Владимир", "владимире": "Владимир",
    "архангельск": "Архангельск", "архангельске": "Архангельск", "чита": "Чита", "чите": "Чита",
    "калуга": "Калуга", "калуге": "Калуга", "смоленск": "Смоленск", "смоленске": "Смоленск",
    "волжский": "Волжский", "череповец": "Череповец", "саранск": "Саранск", "саранске": "Саранск",
    "курган": "Курган", "кургане": "Курган", "вологда": "Вологда", "вологде": "Вологда",
    "орёл": "Орёл", "орел": "Орёл", "якутск": "Якутск", "якутске": "Якутск", "грозный": "Грозный",
    "мурманск": "Мурманск", "мурманске": "Мурманск", "тамбов": "Тамбов", "тамбове": "Тамбов",
    "петрозаводск": "Петрозаводск", "петрозаводске": "Петрозаводск", "кострома": "Кострома", "костроме": "Кострома",
    "нижневартовск": "Нижневартовск", "йошкар ола": "Йошкар-Ола", "новороссийск": "Новороссийск",
    "комсомольск на амуре": "Комсомольск-на-Амуре", "таганрог": "Таганрог", "сыктывкар": "Сыктывкар",
    "нальчик": "Нальчик", "нальчике": "Нальчик", "шахты": "Шахты", "дзержинск": "Дзержинск",
    "братск": "Братск", "орск": "Орск", "ангaрск": "Ангарск", "ангарск": "Ангарск",
    "благовещенск": "Благовещенск", "старый оскол": "Старый Оскол", "великий новгород": "Великий Новгород",
    "псков": "Псков", "пскове": "Псков", "южно сахалинск": "Южно-Сахалинск", "абакан": "Абакан",
    "армавир": "Армавир", "подольск": "Подольск", "королёв": "Королёв", "королев": "Королёв",
    "мытищи": "Мытищи", "химки": "Химки", "люберцы": "Люберцы", "одинцово": "Одинцово",
    "домодедово": "Домодедово", "реутов": "Реутов", "видное": "Видное", "красногорск": "Красногорск",
}

WORK_PATTERNS = [
    r"\bищ(?:у|ем|ут)\s+(?:работ|подработ|сотрудник|рабоч|люд|персонал|бригад|мастер)",
    r"\bнужн(?:а|ы|о|ен)\b", r"\bтребу(?:ется|ются)\b", r"\bнабира(?:ем|ют)\b",
    r"\bваканси(?:я|и|ю|й)\b", r"\bесть\s+работа\b", r"\bработа\s+для\b",
    r"\bвыход\s+(?:сегодня|завтра|на смену)\b", r"\bоплата\b", r"\bзарплат",
    r"\bсмена\b", r"\bвахта\b", r"\bобъект\b", r"\bбригада\b",
]
WORK_RE = re.compile("|".join(WORK_PATTERNS), re.IGNORECASE)


def _normalize_search_text(text: str) -> str:
    value = text.lower().replace("ё", "е")
    value = re.sub(r"[\n\r\t,.;:!?()\[\]{}]+", " ", value)
    value = re.sub(r"[-–—_/]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _application_fingerprint_text(text: str) -> str:
    """Нормализует объявление для поиска дублей между разными беседами.

    Убираются служебные упоминания @all/@online и различия в пробелах/регистре,
    поэтому одно и то же объявление не попадёт в модерацию повторно даже при
    пересылке в другой чат.
    """
    value = re.sub(r"(?<![\w])@(?:all|online)(?![\w])", " ", str(text or ""), flags=re.IGNORECASE)
    value = _normalize_search_text(value)
    return re.sub(r"\s+", " ", value).strip()


def _is_own_group_sender(user_id: Any) -> bool:
    """Возвращает True только для сообщений, отправленных этим сообществом."""
    try:
        sender_id = int(user_id or 0)
    except (TypeError, ValueError):
        return False
    return sender_id in {int(group_id), -abs(int(group_id))}


def _is_meaningful_monitored_text(text: str) -> bool:
    """Мягкий фильтр для чатов, которым назначен город через .устгород.

    В таких чатах приоритет — не потерять заявку, поэтому принимается почти
    любое содержательное сообщение. Отбрасываются только пустые/совсем короткие
    строки и команды бота.
    """
    raw = str(text or "").strip()
    if not raw:
        return False
    command_text = normalize_group_chat_command(raw)
    if command_text.startswith("."):
        return False
    normalized = _normalize_search_text(raw)
    if len(normalized) < 6:
        return False
    return bool(re.search(r"[а-яa-z0-9]", normalized, re.IGNORECASE))


def detect_city(text: str) -> str | None:
    normalized = _normalize_search_text(text)
    # Сначала длинные варианты, чтобы «ростов-на-дону» не превращался просто в «Ростов».
    for alias in sorted(CITY_ALIASES, key=len, reverse=True):
        normalized_alias = _normalize_search_text(alias)
        if re.search(rf"(?<![а-яa-z0-9]){re.escape(normalized_alias)}(?![а-яa-z0-9])", normalized):
            return CITY_ALIASES[alias]
    return None


def is_work_application_text(text: str) -> bool:
    """Определяет объявления, в которых набирают людей на работу/подработку.

    Фильтр учитывает как обычные фразы ("нужны грузчики"), так и
    структурированные объявления без слова "нужны": дата, время, адрес,
    количество людей, оплата и описание работ. Город проверяется отдельно.
    """
    normalized = _normalize_search_text(text)
    if len(normalized) < 12 or len(normalized) > 5000:
        return False

    # Сразу отсекаем сообщения соискателей, которые сами ищут работу.
    negative_patterns = (
        r"\bищу\s+(?:себе\s+)?(?:работу|подработку|вакансию|заказы)\b",
        r"\bищем\s+(?:себе\s+)?(?:работу|подработку|заказы)\b",
        r"\bмне\s+нужн(?:а|ы)\s+(?:работа|подработка)\b",
        r"\bнужн(?:а|ы)\s+(?:работа|подработка)\b",
        r"\bпоиск\s+(?:работы|вакансии)\b",
        r"\bрассмотрю\s+(?:работу|вакансии|предложения)\b",
        r"\bготов(?:а|ы)?\s+выйти\s+на\s+работу\b",
        r"\bкто\s+может\s+предложить\s+работу\b",
    )
    if any(re.search(pattern, normalized) for pattern in negative_patterns):
        return False

    # Прямое указание, что работодатель набирает людей.
    employer_patterns = (
        r"\bищ(?:ем|ут)\s+(?:\d+\s+)?(?:сотрудник|работник|рабоч|люд|человек|персонал|бригад|мастер|специалист|помощник|подсобник|мужчин|девуш)",
        r"\bнужн(?:ы|ен|а|о)\s+(?:еще\s+)?(?:\d+\s+)?(?:сотрудник|работник|рабоч|люд|человек|человека|персонал|бригад|мастер|специалист|помощник|подсобник|мужчин|девуш)",
        r"\bтребу(?:ется|ются)\b",
        r"\bнабира(?:ем|ют)\b",
        r"\bоткрыт\w*\s+ваканси",
        r"\bкто\s+(?:готов|может)\s+выйти\b",
        r"\bвыход\s+(?:сегодня|завтра|на\s+смену)\b",
    )
    strong_intent = any(re.search(pattern, normalized) for pattern in employer_patterns)

    # Количество исполнителей часто указывают без слов "нужны".
    people_patterns = (
        r"\bкол(?:\s*[-–—]?\s*во|ичество)\s*(?:чел(?:овек)?|людей)?\s*[:\-]?\s*\d+",
        r"\b(?:еще|всего)\s*\d+\b",
        r"\b\d+\s*(?:чел(?:\.|овек(?:а|у|ов)?)?|людей|мужчин|девуш(?:ки|ек)?)\b",
        r"\b(?:мужчины|мужчин|девушки|женщины)\b",
    )
    has_people = any(re.search(pattern, normalized) for pattern in people_patterns)

    # Типичные заголовки и поля шаблонных объявлений.
    has_work_header = bool(re.search(
        r"(?:\badwork\b|\bработа\b|\bподработка\b|\bваканси(?:я|и|ю|й)\b|"
        r"\bвид\s+работы\b|\bописание\s+работ\b)",
        normalized,
    ))
    structured_fields = (
        r"\bдата\s*[:\-]",
        r"\bвремя\s*[:\-]",
        r"\bадрес\s*[:\-]",
        r"\bрайон\s*[:\-]",
        r"\bоплат(?:а|а\s*/\s*час|а\s+за\s+час)\s*[:\-]",
        r"\bкол(?:\s*[-–—]?\s*во|ичество)\s+(?:чел|людей)",
        r"\bописание\s+работ\s*[:\-]",
        r"\bвид\s+работы\s*[:\-]",
    )
    structured_count = sum(bool(re.search(pattern, normalized)) for pattern in structured_fields)

    # Оплата: 3600р, 450 ₽ час, 300ч/2мин, 350 руб/час и т.п.
    has_payment = bool(re.search(
        r"(?:\bоплат|\bставк|\bзарплат|"
        r"\b\d{2,6}\s*(?:₽|руб(?:\.|ля|лей)?|р(?:\.|\b)|р\s*/\s*ч|рч)|"
        r"\b\d{2,6}\s*(?:за|в)?\s*(?:час|смену|день)\b|"
        r"\b\d{2,4}\s*ч\s*/\s*\d+\s*мин)",
        normalized,
    ))

    # Дата/время выполнения.
    has_schedule = bool(re.search(
        r"(?:\bсегодня\b|\bзавтра\b|\bпослезавтра\b|"
        r"\b\d{1,2}[.:]\d{2}\b|\bс\s*\d{1,2}(?:[.:]\d{2})?\s*до\s*\d{1,2}|"
        r"\bна\s+\d{1,2}(?:\s*[,\-]\s*\d{1,2})?\s+[а-я]+)",
        normalized,
    ))

    # Адрес или явное место выполнения.
    has_address = bool(re.search(
        r"(?:\bадрес\b|\bулиц(?:а|е|у|ы)\b|\bул\.?\s+[а-я]|\bпроезд\b|"
        r"\bпроспект\b|\bпр\-?т\b|\bшоссе\b|\bпереулок\b|\bторговый\s+центр\b|\bтц\b)",
        normalized,
    ))

    # Не ограничиваемся профессиями: разовые задания часто описывают действиями.
    task_terms = (
        "монтаж", "установ", "швелер", "балк", "сва", "свар", "электр",
        "разнораб", "грузчик", "разгруз", "погруз", "выгруж", "загруж",
        "перенос", "перемещ", "подъем", "поднять", "помочь", "помощь",
        "измельч", "блокорез", "навеск", "формовк", "производств", "цех",
        "упаков", "комплект", "склад", "палет", "обматыв", "двер", "мешк",
        "рулон", "мебел", "товар", "уборк", "демонтаж", "строй", "бетон",
        "арматур", "маляр", "каменщик", "плотник", "штукатур", "сантех",
        "слесар", "курьер", "водител", "охранник", "кассир", "повар",
    )
    has_task = any(term in normalized for term in task_terms)

    has_contact_call = bool(re.search(
        r"(?:\bпиш(?:ите|ем)\s+(?:мне\s+)?в\s+личн|\bдля\s+принятия\s+заказа|"
        r"\bномер\s+телефона|\bимя\s+фамили)",
        normalized,
    ))

    # Балльная модель нужна для объявлений-шаблонов, где нет фразы "нужны люди".
    score = 0
    if strong_intent:
        score += 6
    if has_people:
        score += 3
    if has_work_header:
        score += 2
    score += min(structured_count, 4)
    if has_payment:
        score += 2
    if has_task:
        score += 2
    if has_schedule:
        score += 1
    if has_address:
        score += 1
    if has_contact_call:
        score += 1

    # Прямой набор людей принимаем сразу. Для шаблонных публикаций требуется
    # несколько независимых признаков, чтобы не ловить обычную переписку.
    if strong_intent:
        return True

    core_features = sum((has_people, has_payment, has_task, has_work_header, structured_count >= 2))
    return score >= 7 and core_features >= 3

def normalize_group_chat_command(value: Any) -> str:
    """Убирает упоминание сообщества перед командой в беседе.

    В некоторых беседах без прав администратора VK может передавать боту
    только сообщения, в которых сообщество упомянуто. Поэтому поддерживаются
    оба варианта: `.устгород Москва` и `[club123|Бот] .устгород Москва`.
    """
    clean = str(value or "").strip()
    clean = re.sub(
        r"^\[(?:club|public)\d+\|[^\]]+\]\s*[,!:;\-—]?\s*",
        "",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(
        r"^@(?:club|public)?\d+\s*[,!:;\-—]?\s*",
        "",
        clean,
        flags=re.IGNORECASE,
    )
    return clean.strip()

def load_applications() -> dict[str, Any]:
    data = read_json(APPLICATIONS_PATH, {"next_id": 1, "items": []})
    if not isinstance(data, dict):
        data = {"next_id": 1, "items": []}
    if not isinstance(data.get("items"), list):
        data["items"] = []
    data["next_id"] = int(data.get("next_id", 1) or 1)
    return data


def save_applications(data: dict[str, Any]) -> None:
    write_json_atomic(APPLICATIONS_PATH, data)


def get_applications_chat() -> int | None:
    value = runtime_data.get("applications_chat")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def get_chat_city_overrides() -> dict[str, str]:
    value = runtime_data.get("chat_city_overrides", {})
    return value if isinstance(value, dict) else {}


def cache_chat_title(peer_id: int, title: str, *, source: str = "api") -> str | None:
    """Сохранить название беседы, полученное законным способом из API или Long Poll."""
    clean_title = str(title or "").strip()
    if not clean_title:
        return None
    registry = get_chat_registry()
    key = str(peer_id)
    entry = dict(registry.get(key) or {})
    entry["chat_id"] = peer_id
    entry["title"] = clean_title
    entry["title_source"] = source
    entry["updated_at"] = now_msk_str()
    registry[key] = entry
    runtime_data["chat_registry"] = registry
    save_runtime_data()
    try:
        _update_invite_log_title(peer_id, clean_title)
    except Exception:
        pass
    return clean_title


def get_cached_chat_title(peer_id: int) -> str | None:
    """Возвращает только кэш; сетевое обновление запускается отдельно в фоне."""
    registry = get_chat_registry()
    entry = registry.get(str(peer_id)) or {}
    title = str(entry.get("title") or "").strip()
    if title:
        return title
    request_chat_registry_refresh(peer_id, force=False)
    return None
def format_application_time(timestamp: Any) -> str:
    try:
        dt = datetime.fromtimestamp(float(timestamp), tz=MSK)
    except (TypeError, ValueError, OSError):
        dt = now_msk()
    return dt.strftime("%d.%m.%Y %H:%M МСК")


def build_application_keyboard(application_id: int) -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "Подтвердить", color=VkKeyboardColor.POSITIVE,
        payload={"command": "application_approve", "application_id": application_id},
    )
    keyboard.add_callback_button(
        "Отклонить", color=VkKeyboardColor.NEGATIVE,
        payload={"command": "application_reject", "application_id": application_id},
    )
    return keyboard.get_keyboard()


def render_application_text(item: dict[str, Any], *, approved: bool = False) -> str:
    sent_time = item.get("source_time") or format_application_time(item.get("source_ts") or item.get("created_ts"))
    if approved:
        return (
            f"{item.get('text', '').strip()}\n\n"
            f"Работодатель: {item.get('author_mention', 'Пользователь VK')}\n"
            f"Город: {item.get('city', 'не определён')}\n"
            f"Время: {sent_time}"
        )

    source_title = str(item.get("source_chat_title") or "Не удалось определить").strip()
    city_source = item.get("city_source")
    if city_source == "chat_title":
        city_note = " (из названия беседы)"
    elif city_source == "manual":
        city_note = " (задан вручную для этой беседы)"
    elif city_source == "unknown":
        city_note = " (нужно проверить вручную)"
    else:
        city_note = ""
    return (
        "Новая заявка\n\n"
        f"Заявка от — {item.get('author_mention', 'Пользователь VK')}\n"
        f"Беседа: {source_title}\n"
        f"ID беседы: {item.get('source_peer_id', 'неизвестен')}\n"
        f"Время: {sent_time}\n\n"
        f"{item.get('text', '').strip()}\n\n"
        f"Город: {item.get('city', 'не определён')}{city_note}"
    )



def _job_city_key(value: Any) -> str:
    """Нормализованное название города для сравнения подписок и заявок."""
    detected = detect_city(str(value or ""))
    return _normalize_search_text(detected or value)


def get_active_applications_for_city(city: str) -> list[dict[str, Any]]:
    """Возвращает только ещё не истёкшие опубликованные объявления города."""
    city_key = _job_city_key(city)
    if not city_key:
        return []
    now_ts = time.time()
    with applications_lock:
        data = load_applications()
        result: list[dict[str, Any]] = []
        for item in data.get("items", []):
            if item.get("status") != "published":
                continue
            if _job_city_key(item.get("city")) != city_key:
                continue
            expires_ts = float(item.get("expires_ts", 0) or 0)
            if not expires_ts:
                published_ts = float(item.get("published_ts", 0) or 0)
                expires_ts = published_ts + APPLICATION_LIFETIME_SECONDS if published_ts else 0
            if expires_ts and expires_ts <= now_ts:
                continue
            if not int(item.get("published_peer_id", 0) or 0):
                continue
            if not int(item.get("published_message_id", 0) or 0):
                continue
            result.append(dict(item))
    result.sort(key=lambda value: float(value.get("published_ts", 0) or 0))
    return result


def get_all_active_applications() -> list[dict[str, Any]]:
    """Возвращает все ещё активные подтверждённые объявления.

    В личных сообщениях они пересылаются пользователю как настоящие сообщения
    из чата публикации, без выбора города и без создания копий.
    """
    now_ts = time.time()
    with applications_lock:
        data = load_applications()
        result: list[dict[str, Any]] = []
        for item in data.get("items", []):
            if item.get("status") != "published":
                continue
            expires_ts = float(item.get("expires_ts", 0) or 0)
            if not expires_ts:
                published_ts = float(item.get("published_ts", 0) or 0)
                expires_ts = published_ts + APPLICATION_LIFETIME_SECONDS if published_ts else 0
            if expires_ts and expires_ts <= now_ts:
                continue
            if not int(item.get("published_peer_id", 0) or 0):
                continue
            if not int(item.get("published_message_id", 0) or 0):
                continue
            result.append(dict(item))
    result.sort(key=lambda value: float(value.get("published_ts", 0) or 0))
    return result



def load_companies() -> dict[str, Any]:
    """Загружает внутренний список компаний. Пользователям он не показывается."""
    data = read_json(COMPANIES_PATH, {"next_id": 1, "items": []})
    if not isinstance(data, dict):
        data = {"next_id": 1, "items": []}
    if not isinstance(data.get("items"), list):
        data["items"] = []
    try:
        data["next_id"] = max(1, int(data.get("next_id", 1) or 1))
    except (TypeError, ValueError):
        data["next_id"] = 1
    return data


def save_companies(data: dict[str, Any]) -> None:
    write_json_atomic(COMPANIES_PATH, data)


def get_companies() -> list[dict[str, Any]]:
    with companies_lock:
        data = load_companies()
        companies = [dict(item) for item in data.get("items", [])]
    companies.sort(key=lambda item: int(item.get("id", 0) or 0))
    return companies


def find_company(identifier: Any) -> dict[str, Any] | None:
    needle = str(identifier or "").strip()
    if not needle:
        return None
    try:
        wanted_id = int(needle)
    except (TypeError, ValueError):
        wanted_id = 0
    normalized_name = _normalize_search_text(needle)
    with companies_lock:
        data = load_companies()
        for item in data.get("items", []):
            same_id = wanted_id and int(item.get("id", 0) or 0) == wanted_id
            same_name = normalized_name and _normalize_search_text(item.get("name")) == normalized_name
            if same_id or same_name:
                return dict(item)
    return None


def create_company(name: str, user_id: int) -> tuple[dict[str, Any] | None, str | None]:
    clean_name = " ".join(str(name or "").strip().split())
    if len(clean_name) < 2:
        return None, "Название компании слишком короткое."
    if len(clean_name) > 120:
        return None, "Название компании слишком длинное. Максимум 120 символов."
    normalized_name = _normalize_search_text(clean_name)
    with companies_lock:
        data = load_companies()
        for item in data.get("items", []):
            if _normalize_search_text(item.get("name")) == normalized_name:
                return None, f"Компания уже существует: №{item.get('id')} — {item.get('name')}"
        company_id = int(data.get("next_id", 1) or 1)
        company = {
            "id": company_id,
            "name": clean_name,
            "created_at": now_msk_str(),
            "created_by": int(user_id),
        }
        data["next_id"] = company_id + 1
        data["items"].append(company)
        save_companies(data)
    return dict(company), None


def delete_company(identifier: str) -> dict[str, Any] | None:
    needle = str(identifier or "").strip()
    if not needle:
        return None
    try:
        wanted_id = int(needle)
    except (TypeError, ValueError):
        wanted_id = 0
    normalized_name = _normalize_search_text(needle)
    with companies_lock:
        data = load_companies()
        for index, item in enumerate(data.get("items", [])):
            same_id = wanted_id and int(item.get("id", 0) or 0) == wanted_id
            same_name = normalized_name and _normalize_search_text(item.get("name")) == normalized_name
            if not (same_id or same_name):
                continue
            removed = dict(data["items"].pop(index))
            save_companies(data)
            return removed
    return None


def render_companies_admin_list() -> str:
    companies = get_companies()
    if not companies:
        return (
            "Список компаний пуст.\n\n"
            "Создание: .создатькомпанию <Название>"
        )
    active_jobs = get_active_pm_jobs()
    job_counts: dict[int, int] = {}
    for job in active_jobs:
        company_id = int(job.get("company_id", 0) or 0)
        if company_id:
            job_counts[company_id] = job_counts.get(company_id, 0) + 1
    lines = ["Компании (видны только администраторам):"]
    for company in companies:
        company_id = int(company.get("id", 0) or 0)
        name = str(company.get("name") or f"Компания №{company_id}").strip()
        lines.append(f"{company_id}. {name} — активных работ: {job_counts.get(company_id, 0)}")
    lines.append("\nУдаление: .удкомпания <номер или название>")
    return "\n".join(lines)


def _companies_page(page: int) -> tuple[list[dict[str, Any]], int, int]:
    companies = get_companies()
    page_count = max(1, (len(companies) + COMPANIES_PAGE_SIZE - 1) // COMPANIES_PAGE_SIZE)
    safe_page = max(0, min(int(page or 0), page_count - 1))
    start_index = safe_page * COMPANIES_PAGE_SIZE
    return companies[start_index:start_index + COMPANIES_PAGE_SIZE], safe_page, page_count


def build_pm_company_selection_keyboard(page: int = 0) -> str | None:
    page_items, safe_page, page_count = _companies_page(page)
    if not page_items:
        return None
    keyboard = VkKeyboard(inline=True)
    for index, company in enumerate(page_items):
        company_id = int(company.get("id", 0) or 0)
        title = str(company.get("name") or f"Компания №{company_id}").strip()
        if len(title) > 40:
            title = title[:37].rstrip() + "..."
        keyboard.add_callback_button(
            title,
            color=VkKeyboardColor.PRIMARY,
            payload={
                "command": "pm_company_select",
                "company_id": company_id,
                "page": safe_page,
            },
        )
        if index < len(page_items) - 1 or page_count > 1:
            keyboard.add_line()
    if page_count > 1:
        if safe_page > 0:
            keyboard.add_callback_button(
                "← Назад",
                color=VkKeyboardColor.SECONDARY,
                payload={"command": "pm_company_page", "page": safe_page - 1},
            )
        if safe_page < page_count - 1:
            keyboard.add_callback_button(
                "Далее →",
                color=VkKeyboardColor.SECONDARY,
                payload={"command": "pm_company_page", "page": safe_page + 1},
            )
    return keyboard.get_keyboard()


def send_pm_company_selection(chat_id: int, page: int = 0) -> bool:
    page_items, safe_page, page_count = _companies_page(page)
    if not page_items:
        send_message(
            chat_id,
            "Сначала создайте хотя бы одну компанию:\n.создатькомпанию <Название>",
            rate_limit=False,
        )
        return False
    send_message(
        chat_id,
        "Выберите компанию, от которой будет добавлена работа.\n"
        "Пользователям название компании показываться не будет.\n\n"
        f"Страница {safe_page + 1} из {page_count}\n"
        "Для отмены: .отмена",
        keyboard=build_pm_company_selection_keyboard(safe_page),
        rate_limit=False,
    )
    return True


def _set_pm_job_creation_company(user_id: int, company: dict[str, Any]) -> bool:
    """Сохраняет выбранную компанию только для сценария .добработа."""
    state = USER_STATES.get(str(user_id)) or {}
    creation = state.get("pm_job_create")
    if not isinstance(creation, dict):
        return False
    creation["company_id"] = int(company.get("id", 0) or 0)
    creation["company_name"] = str(company.get("name") or "Не указана").strip()
    creation["step"] = "title"
    state["pm_job_create"] = creation
    USER_STATES[str(user_id)] = state
    return True


def handle_pm_job_admin_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    command = str(payload.get("command") or "").strip().lower()
    if command not in {"pm_company_select", "pm_company_page", "pm_job_format_select"}:
        return False

    event_object = payload.get("_event_object")
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Недостаточно прав.")
        return True

    state = USER_STATES.get(str(user_id)) or {}

    # Формат работы относится и к .добработа, и к .измработа.
    # Его нужно обрабатывать ДО проверки шага выбора компании.
    if command == "pm_job_format_select":
        format_key = _normalize_pm_job_work_format(payload.get("format"))
        if not format_key:
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Неизвестный формат.")
            return True

        mode = str(payload.get("mode") or "").strip().lower()

        if mode == "create":
            creation = state.get("pm_job_create")
            if not isinstance(creation, dict) or str(creation.get("step") or "") != "work_format":
                if isinstance(event_object, dict):
                    answer_callback_event(event_object, text="Выбор устарел.")
                send_message(chat_id, "Выбор формата устарел. Начните заново командой .добработа.")
                return True

            creation["work_format"] = format_key
            creation["step"] = "description"
            state["pm_job_create"] = creation
            USER_STATES[str(user_id)] = state

            if isinstance(event_object, dict):
                answer_callback_event(event_object, text=PM_JOB_WORK_FORMATS[format_key])

            send_message(
                chat_id,
                f"Формат выбран: {PM_JOB_WORK_FORMATS[format_key]}\n\n"
                "Теперь отправьте полное описание работы одним сообщением. "
                "К этому сообщению можно прикрепить фото.\n\nДля отмены: .отмена",
                rate_limit=False,
            )
            return True

        if mode == "edit":
            editing = state.get("pm_job_edit")
            job_id = int(payload.get("job_id") or 0)

            if (
                not isinstance(editing, dict)
                or str(editing.get("step") or "") != "work_format"
                or int(editing.get("job_id") or 0) != job_id
            ):
                if isinstance(event_object, dict):
                    answer_callback_event(event_object, text="Выбор устарел.")
                send_message(chat_id, "Выбор формата устарел. Используйте .измработа <номер> заново.")
                return True

            updated = update_pm_job(job_id, {"work_format": format_key})
            if not updated:
                if isinstance(event_object, dict):
                    answer_callback_event(event_object, text="Работа не найдена.")
                send_message(chat_id, "Работа не найдена.")
                return True

            editing["step"] = "description"
            state["pm_job_edit"] = editing
            USER_STATES[str(user_id)] = state

            if isinstance(event_object, dict):
                answer_callback_event(event_object, text=PM_JOB_WORK_FORMATS[format_key])

            send_message(
                chat_id,
                f"Формат установлен: {PM_JOB_WORK_FORMATS[format_key]}\n\n"
                "Теперь отправьте новый текст работы. К сообщению можно прикрепить новое фото.\n\n"
                ".оставить — оставить старый текст и фото\n"
                ".убратьфото — оставить текст, но удалить фото\n"
                "Сообщение только с фото — заменить только фото\n"
                "Новый текст без фото — заменить текст, старое фото оставить\n"
                ".отмена — отменить редактирование",
                rate_limit=False,
            )
            return True

        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Неизвестный режим.")
        return True

    # Ниже — только выбор компании для .добработа.
    creation = state.get("pm_job_create")
    if not isinstance(creation, dict) or str(creation.get("step") or "") != "company":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Сначала используйте .добработа")
        send_message(chat_id, "Выбор компании устарел. Начните заново командой .добработа.")
        return True

    if command == "pm_company_page":
        try:
            page = max(0, int(payload.get("page", 0) or 0))
        except (TypeError, ValueError):
            page = 0
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Показываю компании.")
        send_pm_company_selection(chat_id, page)
        return True

    company = find_company(payload.get("company_id"))
    if not company:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Компания уже удалена.")
        send_pm_company_selection(chat_id, 0)
        return True

    _set_pm_job_creation_company(user_id, company)
    if isinstance(event_object, dict):
        answer_callback_event(event_object, text="Компания выбрана.")
    send_message(
        chat_id,
        f"Компания выбрана: {company.get('name')}\n\n"
        "Теперь отправьте название работы. Оно будет отображаться на кнопке в личных сообщениях.\n\n"
        "Для отмены: .отмена",
        rate_limit=False,
    )
    return True


def load_pm_jobs() -> dict[str, Any]:
    """Загружает каталог работ, предназначенный только для личных сообщений."""
    data = read_json(PM_JOBS_PATH, {"next_id": 1, "items": []})
    if not isinstance(data, dict):
        data = {"next_id": 1, "items": []}
    if not isinstance(data.get("items"), list):
        data["items"] = []
    try:
        data["next_id"] = max(1, int(data.get("next_id", 1) or 1))
    except (TypeError, ValueError):
        data["next_id"] = 1
    return data


def save_pm_jobs(data: dict[str, Any]) -> None:
    write_json_atomic(PM_JOBS_PATH, data)


def get_active_pm_jobs() -> list[dict[str, Any]]:
    with pm_jobs_lock:
        data = load_pm_jobs()
        jobs = [dict(item) for item in data.get("items", []) if item.get("active", True)]
    jobs.sort(key=lambda item: int(item.get("id", 0) or 0))
    return jobs


def get_pm_job(job_id: int, *, active_only: bool = True) -> dict[str, Any] | None:
    with pm_jobs_lock:
        data = load_pm_jobs()
        for item in data.get("items", []):
            if int(item.get("id", 0) or 0) != int(job_id):
                continue
            if active_only and not item.get("active", True):
                return None
            return dict(item)
    return None


def find_pm_job(identifier: Any, *, active_only: bool = True) -> dict[str, Any] | None:
    needle = str(identifier or "").strip()
    if not needle:
        return None
    try:
        wanted_id = int(needle)
    except (TypeError, ValueError):
        wanted_id = 0
    normalized_title = _normalize_search_text(needle)
    with pm_jobs_lock:
        data = load_pm_jobs()
        for item in data.get("items", []):
            if active_only and not item.get("active", True):
                continue
            same_id = wanted_id and int(item.get("id", 0) or 0) == wanted_id
            same_title = normalized_title and _normalize_search_text(item.get("title")) == normalized_title
            if same_id or same_title:
                return dict(item)
    return None


def update_pm_job(job_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
    with pm_jobs_lock:
        data = load_pm_jobs()
        for item in data.get("items", []):
            if int(item.get("id", 0) or 0) != int(job_id):
                continue
            item.update(updates)
            item["updated_at"] = now_msk_str()
            save_pm_jobs(data)
            return dict(item)
    return None


def _pm_job_location(job: dict[str, Any]) -> str:
    return str(job.get("location") or "").strip()


PM_JOB_WORK_FORMATS = {
    "local": "Только местно",
    "shift": "Только вахта",
    "both": "Вахта/Местно",
}


def _normalize_pm_job_work_format(value: Any) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "local": "local",
        "местно": "local",
        "только местно": "local",
        "shift": "shift",
        "вахта": "shift",
        "только вахта": "shift",
        "both": "both",
        "вахта/местно": "both",
        "вахта / местно": "both",
        "местно/вахта": "both",
        "местно / вахта": "both",
    }
    return aliases.get(raw, raw if raw in PM_JOB_WORK_FORMATS else "")


def _pm_job_work_format(job: dict[str, Any]) -> str:
    key = _normalize_pm_job_work_format(job.get("work_format"))
    return PM_JOB_WORK_FORMATS.get(key, "Формат не указан")


def build_pm_job_work_format_keyboard(*, mode: str, job_id: int = 0) -> str:
    keyboard = VkKeyboard(inline=True)
    entries = [("local", "Только местно"), ("shift", "Только вахта"), ("both", "Вахта/Местно")]
    for idx, (key, label) in enumerate(entries):
        keyboard.add_callback_button(
            label,
            color=VkKeyboardColor.PRIMARY,
            payload={
                "command": "pm_job_format_select",
                "format": key,
                "mode": str(mode),
                "job_id": int(job_id or 0),
            },
        )
        if idx < len(entries) - 1:
            keyboard.add_line()
    return keyboard.get_keyboard()


def build_pm_job_deep_link(job_id: int) -> str:
    """Ссылка на ЛС сообщества с привязкой к конкретной работе."""
    return f"https://vk.me/club{group_id}?ref=job_{int(job_id)}"


def build_pm_jobs_catalog_deep_link() -> str:
    """Ссылка на ЛС сообщества, открывающая общий каталог вакансий."""
    return f"https://vk.me/club{group_id}?ref=jobs"


def extract_pm_job_id_from_link(link: Any) -> int:
    """Извлекает номер работы из нашей ссылки vk.me с ref=job_N."""
    value = str(link or "").strip()
    if not value:
        return 0
    match = re.search(r"(?:[?&]ref=|^|[^a-z0-9])job[_:-]?(\d+)(?:$|[^0-9])", value, flags=re.IGNORECASE)
    if not match:
        return 0
    try:
        return max(0, int(match.group(1)))
    except (TypeError, ValueError):
        return 0


def remember_pending_pm_job(user_id: int, job_id: int, ttl_seconds: int = 24 * 60 * 60) -> None:
    """Запоминает вакансию до первого сообщения пользователя, без фоновых проверок."""
    state = USER_STATES.setdefault(str(int(user_id)), {})
    state["pending_pm_job"] = {
        "job_id": int(job_id),
        "expires_at": time.time() + max(60, int(ttl_seconds)),
    }


def pop_pending_pm_job(user_id: int) -> int:
    """Возвращает ожидающую вакансию один раз и сразу удаляет запись."""
    state = USER_STATES.get(str(int(user_id))) or {}
    pending = state.pop("pending_pm_job", None)
    if not isinstance(pending, dict):
        return 0
    try:
        expires_at = float(pending.get("expires_at", 0) or 0)
        job_id = int(pending.get("job_id", 0) or 0)
    except (TypeError, ValueError):
        return 0
    if not job_id or expires_at < time.time():
        return 0
    return job_id


def extract_pm_job_id_from_ref(message: dict[str, Any] | None) -> int:
    """Извлекает job_id из перехода vk.me?...ref=job_N.

    VK в разных событиях/версиях API может положить метку перехода в ref,
    ref_source или payload, поэтому проверяем все безопасные варианты.
    """
    if not isinstance(message, dict):
        return 0

    candidates: list[Any] = [
        message.get("ref"),
        message.get("ref_source"),
        message.get("start_param"),
    ]
    raw_payload = message.get("payload")
    if isinstance(raw_payload, dict):
        candidates.extend([
            raw_payload.get("ref"),
            raw_payload.get("ref_source"),
            raw_payload.get("start_param"),
        ])
    elif isinstance(raw_payload, str):
        try:
            decoded_payload = json.loads(raw_payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded_payload = None
        if isinstance(decoded_payload, dict):
            candidates.extend([
                decoded_payload.get("ref"),
                decoded_payload.get("ref_source"),
                decoded_payload.get("start_param"),
            ])
        candidates.append(raw_payload)

    for candidate in candidates:
        value = str(candidate or "").strip()
        match = re.search(r"(?:^|[^a-z0-9])job[_:-]?(\d+)(?:$|[^0-9])", value, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            return max(0, int(match.group(1)))
        except (TypeError, ValueError):
            continue
    return 0



def has_pm_jobs_catalog_ref(message: dict[str, Any] | None) -> bool:
    """Проверяет переход в ЛС по ссылке ?ref=jobs / vacancies / work."""
    if not isinstance(message, dict):
        return False
    candidates: list[Any] = [
        message.get("ref"),
        message.get("ref_source"),
        message.get("start_param"),
    ]
    raw_payload = message.get("payload")
    if isinstance(raw_payload, dict):
        candidates.extend([
            raw_payload.get("ref"),
            raw_payload.get("ref_source"),
            raw_payload.get("start_param"),
        ])
    elif isinstance(raw_payload, str):
        try:
            decoded_payload = json.loads(raw_payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded_payload = None
        if isinstance(decoded_payload, dict):
            candidates.extend([
                decoded_payload.get("ref"),
                decoded_payload.get("ref_source"),
                decoded_payload.get("start_param"),
            ])
        candidates.append(raw_payload)

    for candidate in candidates:
        value = str(candidate or "").strip().lower()
        if re.search(r"(?:^|[^a-z0-9])(jobs|vacancies|work)(?:$|[^a-z0-9])", value):
            return True
    return False


def peek_pending_pm_job(user_id: int) -> int:
    """Смотрит ожидающую вакансию, не удаляя её из состояния пользователя."""
    state = USER_STATES.get(str(int(user_id))) or {}
    pending = state.get("pending_pm_job")
    if not isinstance(pending, dict):
        return 0
    try:
        expires_at = float(pending.get("expires_at", 0) or 0)
        job_id = int(pending.get("job_id", 0) or 0)
    except (TypeError, ValueError):
        return 0
    if not job_id or expires_at < time.time():
        return 0
    return job_id


def _normalize_pm_access_target(target: dict[str, Any] | None) -> dict[str, Any]:
    source = target if isinstance(target, dict) else {}
    target_type = str(source.get("target") or "list").strip().lower()
    if target_type not in {"list", "job", "interest"}:
        target_type = "list"
    try:
        page = max(0, int(source.get("page", 0) or 0))
    except (TypeError, ValueError):
        page = 0
    try:
        job_id = max(0, int(source.get("job_id", 0) or 0))
    except (TypeError, ValueError):
        job_id = 0
    result: dict[str, Any] = {"target": target_type, "page": page}
    if job_id:
        result["job_id"] = job_id
    if target_type in {"job", "interest"} and not job_id:
        result["target"] = "list"
    return result


def build_pm_subscription_keyboard(target: dict[str, Any] | None = None) -> str:
    """Кнопка обязательного вступления, теперь с учётом кликов."""
    return build_tracked_openlink_keyboard(
        "Вступить в рабочий чат",
        WORK_CHAT_JOIN_LINK,
        stats_key="pm_subscription_join",
        stats_source="ЛС / обязательное вступление",
        color=VkKeyboardColor.PRIMARY,
    )


def _remember_pm_auto_membership_wait(user_id: int, target: dict[str, Any] | None) -> None:
    safe_target = _normalize_pm_access_target(target)
    with _pm_membership_lock:
        _pm_auto_membership_waiting[int(user_id)] = (
            safe_target,
            time.monotonic() + PM_AUTO_MEMBERSHIP_WAIT_TTL_SECONDS,
        )


def _clear_pm_auto_membership_wait(user_id: int) -> None:
    with _pm_membership_lock:
        _pm_auto_membership_waiting.pop(int(user_id), None)


def send_pm_subscription_required(
    user_id: int,
    target: dict[str, Any] | None = None,
    *,
    check_error: bool = False,
) -> None:
    _remember_pm_auto_membership_wait(user_id, target)
    if check_error:
        text = (
            "Сейчас не удалось проверить вступление в рабочий чат.\n\n"
            "Вступите в чат — бот продолжит проверять автоматически и сам отправит "
            "вакансии, как только увидит вас среди участников."
        )
    else:
        text = (
            "Для использования бота в личных сообщениях необходимо вступить "
            "в наш рабочий чат.\n\n"
            "Ничего подтверждать не нужно: после вступления бот автоматически "
            "проверит участие и в течение нескольких секунд продолжит действие."
        )
    safe_send_pm(
        user_id,
        text,
        keyboard=build_pm_subscription_keyboard(target),
        rate_limit=False,
    )


def _get_cached_pm_membership(user_id: int) -> bool | None:
    if has_permission(user_id, "admin"):
        return True
    now = time.monotonic()
    with _pm_membership_lock:
        cached = _pm_membership_cache.get(int(user_id))
        if not cached:
            return None
        is_member, expires_at = cached
        if expires_at <= now:
            _pm_membership_cache.pop(int(user_id), None)
            return None
        return bool(is_member)


def _set_cached_pm_membership(user_id: int, is_member: bool) -> None:
    with _pm_membership_lock:
        _pm_membership_cache[int(user_id)] = (
            bool(is_member),
            time.monotonic() + PM_MEMBERSHIP_CACHE_TTL_SECONDS,
        )


def _query_required_pm_chat_membership(user_id: int) -> bool | None:
    """Проверяет участника отдельным VK-клиентом, не блокируя Long Poll."""
    if has_permission(user_id, "admin"):
        return True
    try:
        response = get_bulk_vk().messages.getConversationMembers(peer_id=REQUIRED_PM_CHAT_ID)
        if not isinstance(response, dict):
            return None
        wanted_id = int(user_id)
        for item in response.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            try:
                member_id = int(item.get("member_id", 0) or 0)
            except (TypeError, ValueError):
                member_id = 0
            if member_id == wanted_id:
                return True
        # Резервная проверка для разных форматов ответа API.
        for profile in response.get("profiles", []) or []:
            if not isinstance(profile, dict):
                continue
            try:
                profile_id = int(profile.get("id", 0) or 0)
            except (TypeError, ValueError):
                profile_id = 0
            if profile_id == wanted_id:
                return True
        return False
    except Exception as exc:
        logger.warning(
            "Не удалось проверить пользователя %s в обязательном чате %s: %s",
            user_id,
            REQUIRED_PM_CHAT_ID,
            exc,
        )
        return None


def _run_pm_access_check(user_id: int) -> None:
    status = _query_required_pm_chat_membership(user_id)
    if status is not None:
        _set_cached_pm_membership(user_id, status)
    with _pm_membership_lock:
        pending = _pm_access_pending.pop(int(user_id), None)
        _pm_access_inflight.discard(int(user_id))
    if not pending:
        if status is True:
            _clear_pm_auto_membership_wait(user_id)
        return
    on_allowed, target = pending
    if status is True:
        _clear_pm_auto_membership_wait(user_id)
        try:
            on_allowed()
        except Exception as exc:
            logger.exception("Ошибка продолжения действия в ЛС после проверки подписки: %s", exc)
        return
    # Пользователю больше не нужно нажимать кнопку проверки. Запоминаем, что он
    # хотел открыть, и лёгкий фоновый watcher сам продолжит действие после вступления.
    _remember_pm_auto_membership_wait(user_id, target)
    send_pm_subscription_required(user_id, target, check_error=status is None)


def require_pm_chat_access(
    user_id: int,
    on_allowed: Callable[[], None],
    target: dict[str, Any] | None = None,
    *,
    event_object: dict[str, Any] | None = None,
    force_check: bool = False,
) -> bool:
    """Проверяет доступ без остановки основного цикла бота.

    Возвращает True, потому что текущее действие всегда считается обработанным:
    оно либо выполнено сразу, либо поставлено в короткую фоновую проверку.
    """
    safe_target = _normalize_pm_access_target(target)
    cached = None if force_check else _get_cached_pm_membership(user_id)
    if cached is True:
        _clear_pm_auto_membership_wait(user_id)
        on_allowed()
        return True
    if cached is False and not force_check:
        _remember_pm_auto_membership_wait(user_id, safe_target)
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Сначала вступите в рабочий чат. Проверка продолжится автоматически.")
        send_pm_subscription_required(user_id, safe_target)
        return True

    if isinstance(event_object, dict):
        answer_callback_event(event_object, text="Проверяю вступление...")
    with _pm_membership_lock:
        _pm_access_pending[int(user_id)] = (on_allowed, safe_target)
        if int(user_id) in _pm_access_inflight:
            return True
        _pm_access_inflight.add(int(user_id))
    try:
        _pm_access_executor.submit(_run_pm_access_check, int(user_id))
    except Exception as exc:
        with _pm_membership_lock:
            _pm_access_pending.pop(int(user_id), None)
            _pm_access_inflight.discard(int(user_id))
        logger.exception("Не удалось запустить проверку обязательного чата: %s", exc)
        send_pm_subscription_required(user_id, safe_target, check_error=True)
    return True


def _fetch_required_pm_chat_member_ids() -> set[int] | None:
    """Получает участников обязательного чата одним API-запросом."""
    try:
        response = get_bulk_vk().messages.getConversationMembers(peer_id=REQUIRED_PM_CHAT_ID)
        if not isinstance(response, dict):
            return None
        member_ids: set[int] = set()
        for item in response.get("items", []) or []:
            if isinstance(item, dict):
                try:
                    member_id = int(item.get("member_id", 0) or 0)
                except (TypeError, ValueError):
                    member_id = 0
                if member_id > 0:
                    member_ids.add(member_id)
        for profile in response.get("profiles", []) or []:
            if isinstance(profile, dict):
                try:
                    profile_id = int(profile.get("id", 0) or 0)
                except (TypeError, ValueError):
                    profile_id = 0
                if profile_id > 0:
                    member_ids.add(profile_id)
        return member_ids
    except Exception as exc:
        logger.warning("Автопроверка обязательного чата временно не удалась: %s", exc)
        return None


def pm_membership_auto_watch_loop() -> None:
    """Автоматически продолжает ЛС после вступления без ручной кнопки.

    Нагрузку держим минимальной: если никто не ждёт вступления, API вообще не
    вызывается. Если ждут хоть 100 человек — всё равно только один запрос к
    getConversationMembers за цикл, затем локальная проверка множества ID.
    """
    while True:
        try:
            time.sleep(PM_AUTO_MEMBERSHIP_POLL_SECONDS)
            now = time.monotonic()
            with _pm_membership_lock:
                expired = [uid for uid, (_target, expires_at) in _pm_auto_membership_waiting.items() if expires_at <= now]
                for uid in expired:
                    _pm_auto_membership_waiting.pop(uid, None)
                if not _pm_auto_membership_waiting:
                    continue

            member_ids = _fetch_required_pm_chat_member_ids()
            if member_ids is None:
                continue

            joined: list[tuple[int, dict[str, Any]]] = []
            with _pm_membership_lock:
                for uid, (target, expires_at) in list(_pm_auto_membership_waiting.items()):
                    if expires_at <= now:
                        _pm_auto_membership_waiting.pop(uid, None)
                        continue
                    if uid not in member_ids:
                        continue
                    _pm_auto_membership_waiting.pop(uid, None)
                    _pm_membership_cache[uid] = (
                        True,
                        time.monotonic() + PM_MEMBERSHIP_CACHE_TTL_SECONDS,
                    )
                    joined.append((uid, dict(target)))

            for uid, target in joined:
                try:
                    _background_executor.submit(perform_pm_access_target, uid, target)
                except Exception as exc:
                    logger.exception("Не удалось продолжить ЛС после авто-проверки вступления: %s", exc)
        except Exception as exc:
            logger.exception("Ошибка фоновой автопроверки вступления: %s", exc)
            time.sleep(5)


def perform_pm_access_target(user_id: int, target: dict[str, Any] | None) -> None:
    """Продолжает действие после успешной кнопки «Проверить вступление»."""
    safe_target = _normalize_pm_access_target(target)
    target_type = safe_target["target"]
    page = int(safe_target.get("page", 0) or 0)
    job_id = int(safe_target.get("job_id", 0) or 0)
    if target_type == "list":
        send_pm_jobs_catalog(user_id, page)
        return
    job = get_pm_job(job_id)
    if not job:
        safe_send_pm(
            user_id,
            "Эта вакансия уже неактуальна. Показываю остальные доступные работы.",
            rate_limit=False,
        )
        send_pm_jobs_catalog(user_id, 0)
        return
    state = USER_STATES.get(str(int(user_id))) or {}
    state.pop("pending_pm_job", None)
    if target_type == "job":
        if not send_pm_job_details(user_id, job, page):
            remember_pending_pm_job(user_id, job_id)
        return
    handle_job_private_action(
        user_id,
        user_id,
        {"command": "pm_job_interest", "job_id": job_id, "page": page},
        access_verified=True,
    )


def render_pm_jobs_public_catalog(page: int = 0) -> str:
    """Текст каталога строго соответствует кнопкам текущей страницы."""
    page_items, safe_page, page_count = _pm_jobs_page(page)
    jobs = get_active_pm_jobs()
    if not page_items:
        return "Сейчас актуальных работ нет. Попробуйте немного позже."

    start_index = safe_page * PM_JOBS_PAGE_SIZE
    if page_count > 1:
        lines = [
            f"Доступные работы — всего {len(jobs)}",
            f"Страница {safe_page + 1} из {page_count}",
            "",
        ]
    else:
        lines = [f"Доступные работы — всего {len(jobs)}:", ""]

    # В тексте показываем только те вакансии, кнопки которых находятся
    # на этой же странице. Вакансии с соседних страниц сюда не попадают.
    for offset, item in enumerate(page_items):
        index = start_index + offset + 1
        title = str(item.get("title") or f"Работа №{item.get('id')}").strip()
        location = _pm_job_location(item)
        lines.append(f"{index}. {title}")
        if location:
            lines.append(f"📍 {location}")
        lines.append(f"🕒 {_pm_job_work_format(item)}")
        lines.append("")

    lines.append("Нажмите на кнопку с названием работы, чтобы посмотреть полное описание.")
    return "\n".join(lines).strip()


def _pm_jobs_page(page: int) -> tuple[list[dict[str, Any]], int, int]:
    jobs = get_active_pm_jobs()
    page_count = max(1, (len(jobs) + PM_JOBS_PAGE_SIZE - 1) // PM_JOBS_PAGE_SIZE)
    safe_page = max(0, min(int(page or 0), page_count - 1))
    start_index = safe_page * PM_JOBS_PAGE_SIZE
    return jobs[start_index:start_index + PM_JOBS_PAGE_SIZE], safe_page, page_count


def build_pm_jobs_catalog_keyboard(page: int = 0) -> str | None:
    page_items, safe_page, page_count = _pm_jobs_page(page)
    if not page_items:
        return None

    keyboard = VkKeyboard(inline=True)
    for index, item in enumerate(page_items):
        title = str(item.get("title") or f"Работа №{item.get('id')}").strip()
        if len(title) > 40:
            title = title[:37].rstrip() + "..."
        keyboard.add_callback_button(
            title,
            color=VkKeyboardColor.PRIMARY,
            payload={
                "command": "pm_job_open",
                "job_id": int(item.get("id", 0) or 0),
                "page": safe_page,
                "_stats_key": f"pm_job_open:{int(item.get('id', 0) or 0)}",
                "_stats_label": title,
                "_stats_source": "Каталог вакансий",
            },
        )
        if index < len(page_items) - 1 or page_count > 1:
            keyboard.add_line()

    if page_count > 1:
        if safe_page > 0:
            keyboard.add_callback_button(
                "← Назад",
                color=VkKeyboardColor.SECONDARY,
                payload={"command": "pm_job_list", "page": safe_page - 1, "_stats_key": "pm_jobs_prev", "_stats_label": "← Назад", "_stats_source": "Каталог вакансий"},
            )
        if safe_page < page_count - 1:
            keyboard.add_callback_button(
                "Далее →",
                color=VkKeyboardColor.SECONDARY,
                payload={"command": "pm_job_list", "page": safe_page + 1, "_stats_key": "pm_jobs_next", "_stats_label": "Далее →", "_stats_source": "Каталог вакансий"},
            )
    return keyboard.get_keyboard()


def build_pm_job_delivery_keyboard(
    job_id: int,
    title: str = "Подробнее о вакансии",
    *,
    stats_source: str = "Рассылка вакансии",
    stats_key: str | None = None,
) -> str:
    """Кнопка из рассылки с учётом кликов."""
    button_title = str(title or "Подробнее о вакансии")[:40]
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        button_title,
        color=VkKeyboardColor.PRIMARY,
        payload={
            "command": "pm_job_deliver",
            "job_id": int(job_id),
            "_stats_key": stats_key or f"pm_job_deliver:{int(job_id)}",
            "_stats_label": button_title,
            "_stats_source": stats_source,
        },
    )
    return keyboard.get_keyboard()


def build_pm_job_details_keyboard(job_id: int, page: int = 0) -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "Интересно, вызвать Менеджера",
        color=VkKeyboardColor.POSITIVE,
        payload={
            "command": "pm_job_interest",
            "job_id": int(job_id),
            "page": int(page),
            "_stats_key": f"pm_job_interest:{int(job_id)}",
            "_stats_label": "Интересно, вызвать Менеджера",
            "_stats_source": "Карточка вакансии",
        },
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "Посмотреть ещё вакансии",
        color=VkKeyboardColor.PRIMARY,
        payload={
            "command": "pm_job_list",
            "page": int(page),
            "_stats_key": "pm_job_list_more",
            "_stats_label": "Посмотреть ещё вакансии",
            "_stats_source": "Карточка вакансии",
        },
    )
    return keyboard.get_keyboard()


def _pm_callback_message_id(payload: dict[str, Any] | None) -> int:
    """conversation_message_id сообщения с callback-кнопкой, если он известен."""
    if not isinstance(payload, dict):
        return 0
    event_object = payload.get("_event_object")
    candidates: list[Any] = [payload.get("_edit_cmid")]
    if isinstance(event_object, dict):
        candidates.extend([
            event_object.get("conversation_message_id"),
            event_object.get("conversation_message_id"),
        ])
    for value in candidates:
        try:
            cmid = int(value or 0)
        except (TypeError, ValueError):
            cmid = 0
        if cmid > 0:
            return cmid
    return 0


def edit_pm_bot_message(
    user_id: int,
    conversation_message_id: int,
    text: str,
    *,
    keyboard: str | None = None,
    attachment: str | None = None,
) -> bool:
    """Редактирует уже отправленное ботом сообщение в ЛС без создания дублей."""
    if int(conversation_message_id or 0) <= 0:
        return False
    # VK не позволит отредактировать очень длинный текст одним сообщением.
    if len(_split_message(text or " ")) != 1:
        return False
    try:
        params: dict[str, Any] = {
            "peer_id": int(user_id),
            "conversation_message_id": int(conversation_message_id),
            "message": text or " ",
            # Пустая строка убирает фото при возврате из карточки в каталог.
            "attachment": str(attachment or ""),
        }
        if keyboard is not None:
            params["keyboard"] = keyboard
        vk.messages.edit(**params)
        return True
    except Exception as exc:
        logger.info(
            "Не удалось отредактировать сообщение каталога %s для %s: %s",
            conversation_message_id,
            user_id,
            exc,
        )
        return False


def send_pm_jobs_catalog(
    user_id: int,
    page: int = 0,
    *,
    edit_conversation_message_id: int = 0,
) -> bool:
    page_items, safe_page, _page_count = _pm_jobs_page(page)
    if not page_items:
        safe_send_pm(
            user_id,
            "Сейчас актуальных работ нет. Попробуйте немного позже.",
            rate_limit=False,
        )
        return False

    catalog_text = render_pm_jobs_public_catalog(safe_page)
    keyboard = build_pm_jobs_catalog_keyboard(safe_page)
    if edit_conversation_message_id and edit_pm_bot_message(
        user_id,
        edit_conversation_message_id,
        catalog_text,
        keyboard=keyboard,
        attachment=None,
    ):
        return True

    safe_send_pm(
        user_id,
        catalog_text,
        keyboard=keyboard,
        rate_limit=False,
    )
    return True


def send_pm_job_details(
    user_id: int,
    job: dict[str, Any],
    page: int = 0,
    *,
    edit_conversation_message_id: int = 0,
) -> bool:
    title = str(job.get("title") or f"Работа №{job.get('id')}").strip()
    description = str(job.get("description") or "Описание не указано.").strip()
    location = _pm_job_location(job) or "Не указано"
    # Вакансии больше не зависят от исходного сообщения администратора.
    # Для старых записей пытаемся один раз восстановить фото из VK, сохранить
    # локальную копию и перезагрузить её в сообщения сообщества.
    attachment = resolve_pm_job_attachment(job)
    work_format = _pm_job_work_format(job)
    details_text = f"{title}\n\n📍 Место работы: {location}\n🕒 Формат: {work_format}\n\n{description}"
    details_keyboard = build_pm_job_details_keyboard(int(job.get("id", 0) or 0), page)
    if edit_conversation_message_id and edit_pm_bot_message(
        user_id,
        edit_conversation_message_id,
        details_text,
        keyboard=details_keyboard,
        attachment=attachment,
    ):
        return True
    result = safe_send_pm(
        user_id,
        details_text,
        keyboard=details_keyboard,
        attachment=attachment,
        rate_limit=False,
    )
    return result is not None


def handle_job_private_message(
    user_id: int,
    text: str,
    message: dict[str, Any] | None = None,
    *,
    access_verified: bool = False,
) -> bool:
    """Показывает каталог или работу только участникам обязательного чата."""
    ref_job_id = extract_pm_job_id_from_ref(message)
    ref_catalog = has_pm_jobs_catalog_ref(message)
    # Явный переход по ссылке общего каталога важнее старой ожидающей вакансии.
    pending_job_id = 0 if ref_catalog else peek_pending_pm_job(user_id)
    access_target = {
        "target": "job" if (ref_job_id or pending_job_id) else "list",
        "job_id": ref_job_id or pending_job_id,
        "page": 0,
    }
    if not access_verified:
        return require_pm_chat_access(
            user_id,
            lambda: handle_job_private_message(
                user_id,
                text,
                message,
                access_verified=True,
            ),
            access_target,
        )

    if ref_catalog:
        # Переход по общей ссылке вакансий открывает именно каталог, а не старую
        # ожидающую карточку.
        (USER_STATES.get(str(int(user_id))) or {}).pop("pending_pm_job", None)
        send_pm_jobs_catalog(user_id, 0)
        return True

    if ref_job_id:
        # Метка ссылки важнее обычного текста «Начать»: показываем выбранную вакансию.
        (USER_STATES.get(str(int(user_id))) or {}).pop("pending_pm_job", None)
        job = get_pm_job(ref_job_id)
        if job:
            send_pm_job_details(user_id, job, 0)
            return True
        safe_send_pm(
            user_id,
            "Эта вакансия уже неактуальна. Показываю остальные доступные работы.",
            rate_limit=False,
        )
        send_pm_jobs_catalog(user_id, 0)
        return True

    # Если человек нажал кнопку вакансии в беседе, но ЛС были закрыты, ничего
    # в фоне не опрашиваем. Просто ждём первое его сообщение и выдаём работу один раз.
    pending_job_id = pop_pending_pm_job(user_id)
    if pending_job_id:
        pending_job = get_pm_job(pending_job_id)
        if pending_job:
            send_pm_job_details(user_id, pending_job, 0)
            return True
        safe_send_pm(
            user_id,
            "Эта вакансия уже неактуальна. Показываю остальные доступные работы.",
            rate_limit=False,
        )
        send_pm_jobs_catalog(user_id, 0)
        return True

    lowered = _normalize_search_text(str(text or "").strip())
    # Поддержка стандартной кнопки «Начать», даже если VK передал её только в payload.
    if not lowered and isinstance(message, dict):
        raw_start_payload = message.get("payload")
        decoded_start_payload: dict[str, Any] | None = None
        if isinstance(raw_start_payload, dict):
            decoded_start_payload = raw_start_payload
        elif isinstance(raw_start_payload, str):
            try:
                candidate_payload = json.loads(raw_start_payload)
            except (TypeError, ValueError, json.JSONDecodeError):
                candidate_payload = None
            if isinstance(candidate_payload, dict):
                decoded_start_payload = candidate_payload
        if decoded_start_payload:
            payload_start_value = (
                decoded_start_payload.get("command")
                or decoded_start_payload.get("action")
                or decoded_start_payload.get("button")
                or decoded_start_payload.get("text")
            )
            lowered = _normalize_search_text(str(payload_start_value or "").strip())

    work_commands = {
        "начать", "старт", "start", "/start",
        "работа", "работы", "работу", "вакансия", "вакансии",
        "посмотреть работу", "показать работу", "найти работу",
    }
    is_work_request = lowered in work_commands or any(
        lowered.startswith(prefix)
        for prefix in ("работа ", "работы ", "вакансия ", "вакансии ")
    )
    if is_work_request:
        send_pm_jobs_catalog(user_id, 0)
        return True

    safe_send_pm(
        user_id,
        'Напишите «Работа», чтобы открыть список доступных работ.',
        rate_limit=False,
    )
    return True

def handle_job_delivery_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    """Открывает ЛС и выдаёт вакансию либо общий каталог."""
    command = str(payload.get("command") or "").strip().lower()
    if command not in {"pm_job_deliver", "pm_jobs_deliver"}:
        return False

    event_object = payload.get("_event_object")
    if command == "pm_jobs_deliver":
        # Сразу открываем ЛС. Если пользователь уже может получать сообщения и
        # состоит в обязательном чате — каталог прилетит туда автоматически.
        if isinstance(event_object, dict):
            answer_callback_event(event_object, link=build_pm_jobs_catalog_deep_link())

        def deliver_catalog_after_access() -> None:
            send_pm_jobs_catalog(user_id, 0)

        return require_pm_chat_access(
            user_id,
            deliver_catalog_after_access,
            {"target": "list", "page": 0},
        )

    try:
        job_id = int(payload.get("job_id", 0) or 0)
    except (TypeError, ValueError):
        job_id = 0
    job = get_pm_job(job_id)
    if not job:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Эта вакансия уже неактуальна.")
        return True

    # Сразу открываем личные сообщения. Проверка членства и отправка выполняются
    # отдельным коротким пулом, поэтому Long Poll и остальные команды не зависают.
    if isinstance(event_object, dict):
        answer_callback_event(event_object, link=build_pm_job_deep_link(job_id))

    def deliver_after_access() -> None:
        if send_pm_job_details(user_id, job, 0):
            (USER_STATES.get(str(int(user_id))) or {}).pop("pending_pm_job", None)
            return
        # Если VK ещё не разрешает писать пользователю, ждём его первое сообщение.
        remember_pending_pm_job(user_id, job_id)

    return require_pm_chat_access(
        user_id,
        deliver_after_access,
        {"target": "job", "job_id": job_id, "page": 0},
    )

def handle_job_private_action(
    chat_id: int,
    user_id: int,
    payload: dict[str, Any],
    *,
    access_verified: bool = False,
) -> bool:
    command = str(payload.get("command") or "").strip().lower()
    if command not in {"pm_access_check", "pm_job_list", "pm_job_open", "pm_job_interest"}:
        return False

    event_object = payload.get("_event_object")
    if chat_id != user_id:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Кнопка работает только в личных сообщениях.")
        return True

    try:
        page = max(0, int(payload.get("page", 0) or 0))
    except (TypeError, ValueError):
        page = 0
    try:
        job_id = int(payload.get("job_id", 0) or 0)
    except (TypeError, ValueError):
        job_id = 0
    edit_cmid = _pm_callback_message_id(payload)

    if command == "pm_access_check":
        target = _normalize_pm_access_target({
            "target": payload.get("target"),
            "job_id": job_id,
            "page": page,
        })
        return require_pm_chat_access(
            user_id,
            lambda: perform_pm_access_target(user_id, target),
            target,
            event_object=event_object if isinstance(event_object, dict) else None,
            force_check=True,
        )

    target_type = {
        "pm_job_list": "list",
        "pm_job_open": "job",
        "pm_job_interest": "interest",
    }[command]
    access_target = {"target": target_type, "job_id": job_id, "page": page}
    if not access_verified:
        cached = _get_cached_pm_membership(user_id)
        if cached is True:
            return handle_job_private_action(
                chat_id,
                user_id,
                payload,
                access_verified=True,
            )
        # После асинхронной проверки старый callback уже будет отвечен, поэтому
        # продолжение выполняется без повторного sendMessageEventAnswer.
        verified_payload = dict(payload)
        if edit_cmid:
            verified_payload["_edit_cmid"] = edit_cmid
        verified_payload.pop("_event_object", None)
        return require_pm_chat_access(
            user_id,
            lambda: handle_job_private_action(
                chat_id,
                user_id,
                verified_payload,
                access_verified=True,
            ),
            access_target,
            event_object=event_object if isinstance(event_object, dict) else None,
        )

    if command == "pm_job_list":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text=f"Страница {page + 1}.")
        # Следующая/предыдущая страница заменяет текущее сообщение, а не
        # создаёт новое. Текст и кнопки всегда относятся к одной странице.
        send_pm_jobs_catalog(
            user_id,
            page,
            edit_conversation_message_id=edit_cmid,
        )
        return True

    job = get_pm_job(job_id)
    if not job:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Эта работа уже неактуальна.")
        safe_send_pm(
            user_id,
            "Эта работа уже неактуальна. Показываю актуальный список.",
            rate_limit=False,
        )
        send_pm_jobs_catalog(user_id, page)
        return True

    if command == "pm_job_open":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Открываю описание.")
        # Карточка работы также открывается в том же сообщении. Кнопка
        # «Посмотреть ещё вакансии» вернёт пользователя на исходную страницу.
        send_pm_job_details(
            user_id,
            job,
            page,
            edit_conversation_message_id=edit_cmid,
        )
        return True

    title = str(job.get("title") or f"Работа №{job_id}").strip()
    company_name = str(job.get("company_name") or "Не указана").strip()
    location = _pm_job_location(job) or "Не указано"
    user_name = get_user_display_name(user_id)
    user_mention = render_user_mention(user_id)
    if admin_chat:
        send_message(
            admin_chat,
            "Пользователь заинтересовался работой из личных сообщений:\n"
            f"Имя и фамилия: {user_name}\n"
            f"Профиль: {user_mention}\n"
            f"Работа: {title}\n"
            f"Место работы: {location}\n"
            f"Компания: {company_name}\n\n"
            "Срочно ответить HR-менеджеру @makswwy",
            rate_limit=False,
        )
    if isinstance(event_object, dict):
        answer_callback_event(event_object, text="Менеджер уведомлён.")
    safe_send_pm(
        user_id,
        "Ваш интерес передан менеджеру. Ожидайте, скоро с вами свяжется HR-менеджер @makswwy.",
        rate_limit=False,
    )
    return True

def handle_pm_job_admin_input(
    chat_id: int,
    user_id: int,
    text: str,
    message: dict[str, Any],
) -> bool:
    """Создание работы: компания, название, затем полное описание."""
    state = USER_STATES.get(str(user_id)) or {}
    creation = state.get("pm_job_create")
    if not isinstance(creation, dict):
        return False
    if chat_id != int(creation.get("chat_id", 0) or 0):
        return False
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        state.pop("pm_job_create", None)
        send_message(chat_id, "❌ Права администратора больше не подтверждены. Создание работы отменено.")
        return True

    clean_text = str(text or "").strip()
    if clean_text.lower() in {".отмена", "отмена"}:
        state.pop("pm_job_create", None)
        if not state:
            USER_STATES.pop(str(user_id), None)
        send_message(chat_id, "Создание работы отменено.")
        return True

    step = str(creation.get("step") or "company")
    if step == "company":
        company = find_company(clean_text)
        if not company:
            send_message(
                chat_id,
                "Выберите компанию кнопкой ниже или отправьте её номер/точное название.",
                keyboard=build_pm_company_selection_keyboard(0),
                rate_limit=False,
            )
            return True
        _set_pm_job_creation_company(user_id, company)
        send_message(
            chat_id,
            f"Компания выбрана: {company.get('name')}\n\n"
            "Теперь отправьте название работы. Оно будет отображаться на кнопке в личных сообщениях.\n\n"
            "Для отмены: .отмена",
            rate_limit=False,
        )
        return True

    if step == "title":
        if len(clean_text) < 2:
            send_message(chat_id, "Название слишком короткое. Отправьте нормальное название работы.")
            return True
        if len(clean_text) > 120:
            send_message(chat_id, "Название слишком длинное. Максимум 120 символов.")
            return True
        creation["title"] = clean_text
        creation["step"] = "location"
        state["pm_job_create"] = creation
        USER_STATES[str(user_id)] = state
        send_message(
            chat_id,
            "Теперь укажите место работы.\n"
            "Например: Московская область, г. Пушкино\n\n"
            "Чтобы не указывать место, отправьте: -\n"
            "Для отмены: .отмена",
        )
        return True

    if step == "location":
        if len(clean_text) > 180:
            send_message(chat_id, "Место работы слишком длинное. Максимум 180 символов.")
            return True
        creation["location"] = "" if clean_text in {"-", "—"} else clean_text
        creation["step"] = "work_format"
        state["pm_job_create"] = creation
        USER_STATES[str(user_id)] = state
        send_message(
            chat_id,
            "Теперь выберите формат работы. Он обязательно будет показан пользователю в ЛС:",
            keyboard=build_pm_job_work_format_keyboard(mode="create"),
            rate_limit=False,
        )
        return True

    if step == "work_format":
        format_key = _normalize_pm_job_work_format(clean_text)
        if not format_key:
            send_message(
                chat_id,
                "Выберите один из вариантов: Только местно / Только вахта / Вахта/Местно",
                keyboard=build_pm_job_work_format_keyboard(mode="create"),
                rate_limit=False,
            )
            return True
        creation["work_format"] = format_key
        creation["step"] = "description"
        state["pm_job_create"] = creation
        USER_STATES[str(user_id)] = state
        send_message(
            chat_id,
            f"Формат выбран: {PM_JOB_WORK_FORMATS[format_key]}\n\n"
            "Теперь отправьте полное описание работы одним сообщением. "
            "К этому сообщению можно прикрепить фото.\n\nДля отмены: .отмена",
        )
        return True

    if step == "description":
        attachments, attachment_files = prepare_pm_job_attachments(
            message,
            prefix=f"pm_job_new_{int(user_id)}_{int(time.time() * 1000)}",
        )
        if not clean_text and not attachments:
            send_message(chat_id, "Отправьте описание работы текстом или приложите фото.")
            return True
        if len(clean_text) > 12000:
            send_message(chat_id, "Описание слишком длинное. Максимум 12 000 символов.")
            return True

        with pm_jobs_lock:
            data = load_pm_jobs()
            job_id = int(data.get("next_id", 1) or 1)
            data["next_id"] = job_id + 1
            data["items"].append(
                {
                    "id": job_id,
                    "title": str(creation.get("title") or f"Работа №{job_id}").strip(),
                    "description": clean_text or "Подробности уточняйте у менеджера.",
                    "attachment": ",".join(attachments),
                    # Локальная резервная копия фото. Новые фото также сразу
                    # перезагружаются от имени сообщества.
                    "attachment_files": attachment_files,
                    "location": str(creation.get("location") or "").strip(),
                    "work_format": _normalize_pm_job_work_format(creation.get("work_format")),
                    "company_id": int(creation.get("company_id", 0) or 0),
                    "company_name": str(creation.get("company_name") or "Не указана").strip(),
                    "active": True,
                    "created_at": now_msk_str(),
                    "created_by": user_id,
                }
            )
            save_pm_jobs(data)

        state.pop("pm_job_create", None)
        if not state:
            USER_STATES.pop(str(user_id), None)
        job_link = build_pm_job_deep_link(job_id)
        send_message(
            chat_id,
            f"Работа №{job_id} добавлена.\n"
            f"Компания: {creation.get('company_name') or 'Не указана'}\n"
            f"Название: {creation.get('title')}\n"
            f"Место работы: {creation.get('location') or 'Не указано'}\n"
            f"Формат: {PM_JOB_WORK_FORMATS.get(_normalize_pm_job_work_format(creation.get('work_format')), 'Формат не указан')}\n\n"
            f"Прямая ссылка на эту вакансию:\n{job_link}\n\n"
            "По этой ссылке человек откроет ЛС сообщества, а бот покажет именно эту вакансию. "
            "Если человек пишет сообществу впервые, VK может сначала показать кнопку «Начать».\n\n"
            "Пользователям компания не показывается. Работа также отображается в ЛС по слову «Работа».",
            attachment=",".join(attachments) or None,
        )
        return True

    state.pop("pm_job_create", None)
    return False


def handle_pm_job_edit_input(
    chat_id: int,
    user_id: int,
    text: str,
    message: dict[str, Any],
) -> bool:
    state = USER_STATES.get(str(user_id)) or {}
    editing = state.get("pm_job_edit")
    if not isinstance(editing, dict):
        return False
    if chat_id != int(editing.get("chat_id", 0) or 0):
        return False
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        state.pop("pm_job_edit", None)
        send_message(chat_id, "❌ Права администратора больше не подтверждены. Редактирование работы отменено.")
        return True

    clean_text = str(text or "").strip()
    lowered = clean_text.lower()
    if lowered in {".отмена", "отмена"}:
        state.pop("pm_job_edit", None)
        if not state:
            USER_STATES.pop(str(user_id), None)
        send_message(chat_id, "Редактирование работы отменено.")
        return True

    job_id = int(editing.get("job_id", 0) or 0)
    job = get_pm_job(job_id, active_only=False)
    if not job:
        state.pop("pm_job_edit", None)
        if not state:
            USER_STATES.pop(str(user_id), None)
        send_message(chat_id, "Работа не найдена. Редактирование остановлено.")
        return True

    step = str(editing.get("step") or "title")
    if step == "title":
        new_title = str(job.get("title") or f"Работа №{job_id}").strip() if lowered in {".оставить", "оставить"} else clean_text
        if len(new_title) < 2:
            send_message(chat_id, "Название слишком короткое. Отправьте новое название или .оставить")
            return True
        if len(new_title) > 120:
            send_message(chat_id, "Название слишком длинное. Максимум 120 символов.")
            return True
        update_pm_job(job_id, {"title": new_title})
        editing["step"] = "location"
        state["pm_job_edit"] = editing
        USER_STATES[str(user_id)] = state
        send_message(
            chat_id,
            f"Текущее место работы: {_pm_job_location(job) or 'Не указано'}\n\n"
            "Отправьте новое место, например: Московская область, г. Пушкино\n"
            ".оставить — не менять\n"
            ".убрать — удалить место\n"
            ".отмена — отменить редактирование",
        )
        return True

    if step == "location":
        if lowered in {".оставить", "оставить"}:
            new_location = _pm_job_location(job)
        elif lowered in {".убрать", "убрать", "-", "—"}:
            new_location = ""
        else:
            if not clean_text:
                send_message(chat_id, "Отправьте новое место, .оставить или .убрать")
                return True
            if len(clean_text) > 180:
                send_message(chat_id, "Место работы слишком длинное. Максимум 180 символов.")
                return True
            new_location = clean_text
        update_pm_job(job_id, {"location": new_location})
        editing["step"] = "work_format"
        state["pm_job_edit"] = editing
        USER_STATES[str(user_id)] = state
        send_message(
            chat_id,
            f"Текущий формат: {_pm_job_work_format(job)}\n\n"
            "Выберите новый формат работы. Он будет показан пользователю в каталоге и карточке вакансии:",
            keyboard=build_pm_job_work_format_keyboard(mode="edit", job_id=job_id),
            rate_limit=False,
        )
        return True

    if step == "work_format":
        if lowered in {".оставить", "оставить"}:
            format_key = _normalize_pm_job_work_format(job.get("work_format"))
            if not format_key:
                send_message(chat_id, "У этой работы формат ещё не установлен. Выберите один из вариантов ниже.", keyboard=build_pm_job_work_format_keyboard(mode="edit", job_id=job_id), rate_limit=False)
                return True
        else:
            format_key = _normalize_pm_job_work_format(clean_text)
            if not format_key:
                send_message(chat_id, "Выберите: Только местно / Только вахта / Вахта/Местно", keyboard=build_pm_job_work_format_keyboard(mode="edit", job_id=job_id), rate_limit=False)
                return True
            update_pm_job(job_id, {"work_format": format_key})
        editing["step"] = "description"
        state["pm_job_edit"] = editing
        USER_STATES[str(user_id)] = state
        send_message(
            chat_id,
            f"Формат: {PM_JOB_WORK_FORMATS.get(format_key, 'Формат не указан')}\n\n"
            "Теперь отправьте новый текст работы. К сообщению можно прикрепить новое фото.\n\n"
            ".оставить — оставить старый текст и фото\n"
            ".убратьфото — оставить текст, но удалить фото\n"
            "Сообщение только с фото — заменить только фото\n"
            "Новый текст без фото — заменить текст, старое фото оставить\n"
            ".отмена — отменить редактирование",
        )
        return True

    if step == "description":
        attachments, attachment_files = prepare_pm_job_attachments(
            message,
            prefix=f"pm_job_{job_id}_{int(time.time() * 1000)}",
        )
        current_description = str(job.get("description") or "Подробности уточняйте у менеджера.").strip()
        current_attachment = str(job.get("attachment") or "").strip()

        if lowered in {".оставить", "оставить"} and not attachments:
            updates = {}
        elif lowered in {".убратьфото", "убратьфото"} and not attachments:
            updates = {"attachment": "", "attachment_files": []}
        else:
            updates: dict[str, Any] = {}
            if clean_text:
                if len(clean_text) > 12000:
                    send_message(chat_id, "Описание слишком длинное. Максимум 12 000 символов.")
                    return True
                updates["description"] = clean_text
            elif not attachments:
                send_message(chat_id, "Отправьте новый текст/фото либо команду .оставить")
                return True
            if attachments:
                updates["attachment"] = ",".join(attachments)
                updates["attachment_files"] = attachment_files
            elif "description" in updates:
                updates.setdefault("attachment", current_attachment)
            updates.setdefault("description", current_description)

        updated = update_pm_job(job_id, updates) or job
        state.pop("pm_job_edit", None)
        if not state:
            USER_STATES.pop(str(user_id), None)
        send_message(
            chat_id,
            f"Работа №{job_id} обновлена.\n"
            f"Кнопка: {updated.get('title')}\n"
            f"Место работы: {_pm_job_location(updated) or 'Не указано'}\n"
            f"Формат: {_pm_job_work_format(updated)}\n"
            f"Фото: {'есть' if str(updated.get('attachment') or '').strip() else 'нет'}",
            attachment=str(updated.get("attachment") or "").strip() or None,
        )
        return True

    state.pop("pm_job_edit", None)
    return False


def render_pm_jobs_admin_list() -> str:
    jobs = get_active_pm_jobs()
    if not jobs:
        return "Список работ для личных сообщений пуст."
    lines = ["Работы для личных сообщений:"]
    for item in jobs:
        title = str(item.get("title") or f"Работа №{item.get('id')}").strip()
        company_name = str(item.get("company_name") or "Не указана").strip()
        location = _pm_job_location(item) or "Не указано"
        lines.append(
            f"{int(item.get('id', 0) or 0)}. {title}\n"
            f"   Место: {location}\n"
            f"   Формат: {_pm_job_work_format(item)}\n"
            f"   Компания: {company_name}"
        )
    lines.append("\nРедактирование: .измработа <номер> (также работает .редработа)")
    lines.append("Быстро заменить фото: .редрабфото <номер> + приложить фото")
    lines.append("Удаление: .удработа <номер>")
    return "\n".join(lines)


def delete_pm_job(identifier: str) -> dict[str, Any] | None:
    needle = str(identifier or "").strip()
    if not needle:
        return None
    try:
        wanted_id = int(needle)
    except (TypeError, ValueError):
        wanted_id = 0
    normalized_title = _normalize_search_text(needle)

    with pm_jobs_lock:
        data = load_pm_jobs()
        for index, item in enumerate(data.get("items", [])):
            same_id = wanted_id and int(item.get("id", 0) or 0) == wanted_id
            same_title = normalized_title and _normalize_search_text(item.get("title")) == normalized_title
            if not (same_id or same_title):
                continue
            removed = dict(data["items"].pop(index))
            save_pm_jobs(data)
            return removed
    return None


def notify_job_subscribers_for_application(item: dict[str, Any]) -> None:
    """Авторассылка заявок из чатов в ЛС отключена намеренно."""
    return


def _is_duplicate_application(
    data: dict[str, Any],
    peer_id: int,
    source_message_id: int = 0,
    source_cmid: int = 0,
) -> bool:
    """Не теряем одинаковые объявления из разных чатов.

    Дублем считается только ТО ЖЕ исходное сообщение VK.
    Одинаковый текст, повторно опубликованный в другом чате или позже,
    теперь проходит как отдельная заявка.
    """
    peer_id = int(peer_id or 0)
    source_message_id = int(source_message_id or 0)
    source_cmid = int(source_cmid or 0)

    if not peer_id or (not source_message_id and not source_cmid):
        return False

    for item in reversed(data.get("items", [])[-1500:]):
        try:
            if int(item.get("source_peer_id", 0) or 0) != peer_id:
                continue
            old_mid = int(item.get("source_message_id", 0) or 0)
            old_cmid = int(item.get("source_conversation_message_id", 0) or 0)
        except (TypeError, ValueError):
            continue

        if source_message_id and old_mid and old_mid == source_message_id:
            return True
        if source_cmid and old_cmid and old_cmid == source_cmid:
            return True

    return False


def process_possible_application(
    peer_id: int,
    user_id: int,
    text: str,
    message_ts: Any = None,
    is_outgoing: bool = False,
    source_message_id: int = 0,
    source_cmid: int = 0,
    attachment_refs: list[str] | None = None,
) -> None:
    """Максимально надёжно сохраняет сообщение как заявку на ручную проверку."""
    publish_chat = get_applications_chat()

    try:
        peer_id = int(peer_id or 0)
        user_id = int(user_id or 0)
        source_message_id = int(source_message_id or 0)
        source_cmid = int(source_cmid or 0)
    except (TypeError, ValueError):
        return

    if not peer_id or peer_id == admin_chat:
        return
    if publish_chat and peer_id == publish_chat:
        return
    if admin_chat <= 0:
        return
    if is_outgoing or _is_own_group_sender(user_id):
        return

    raw_text = str(text or "").strip()
    attachment_refs = [
        str(value).strip()
        for value in (attachment_refs or [])
        if str(value or "").strip()
    ]

    # Теперь не теряем даже сообщения, где заявка пришла только во вложении.
    if not raw_text and attachment_refs:
        raw_text = "Сообщение без текста — проверьте приложенное вложение вручную."
    if not raw_text:
        return

    manual_city = str(get_chat_city_overrides().get(str(peer_id)) or "").strip()

    city = detect_city(raw_text)
    city_source = "message"
    if not city and manual_city:
        city = manual_city
        city_source = "manual"

    source_chat_title = get_cached_chat_title(peer_id)
    if not city and source_chat_title:
        city = detect_city(source_chat_title)
        city_source = "chat_title"

    if not city:
        city = "Не определён"
        city_source = "unknown"

    source_ts = float(message_ts or time.time())

    # Сетевые запросы имени делаем ДО applications_lock.
    author_mention = (
        render_application_author_mention(peer_id, user_id)
        if user_id != 0
        else "Отправитель не определён"
    )

    item: dict[str, Any] | None = None

    try:
        # В lock теперь только быстрые операции с JSON.
        with applications_lock:
            data = load_applications()

            if _is_duplicate_application(
                data,
                peer_id,
                source_message_id=source_message_id,
                source_cmid=source_cmid,
            ):
                return

            application_id = int(data["next_id"])
            data["next_id"] = application_id + 1

            item = {
                "id": application_id,
                "status": "pending",
                "source_peer_id": peer_id,
                "source_message_id": source_message_id,
                "source_conversation_message_id": source_cmid,
                "user_id": user_id,
                "author_mention": author_mention,
                "text": raw_text,
                "normalized_text": _normalize_search_text(raw_text),
                "text_fingerprint": _application_fingerprint_text(raw_text),
                "city": city,
                "city_source": city_source,
                "source_chat_title": source_chat_title or "",
                "source_ts": source_ts,
                "source_time": format_application_time(source_ts),
                "created_ts": time.time(),
                "created_at": now_msk_str(),
                "attachment_refs": attachment_refs,
                "review_message_id": 0,
                "delivery_status": "pending",
            }

            # Сохраняем СРАЗУ, до отправки в админ-чат.
            # Даже если VK временно не даст отправить уведомление, заявка не потеряется.
            data["items"].append(item)
            save_applications(data)

        attachment_value = ",".join(attachment_refs[:10]) if attachment_refs else None

        result = None
        for attempt in range(1, 4):
            result = send_message(
                admin_chat,
                render_application_text(item),
                attachment=attachment_value,
                keyboard=build_application_keyboard(application_id),
                rate_limit=False,
                api_client=get_bulk_vk(),
            )
            if result not in {None, "access_error"}:
                break
            if attempt < 3:
                time.sleep(0.5 * attempt)

        with applications_lock:
            data = load_applications()
            saved_item = next(
                (x for x in data["items"] if int(x.get("id", 0) or 0) == application_id),
                None,
            )
            if saved_item is not None:
                if result not in {None, "access_error"}:
                    saved_item["review_message_id"] = int(result)
                    saved_item["delivery_status"] = "sent"
                    saved_item["delivered_at"] = now_msk_str()
                else:
                    saved_item["delivery_status"] = "failed"
                    saved_item["delivery_error_at"] = now_msk_str()
                save_applications(data)

        logger.info(
            "Заявка #%s сохранена из чата %s cmid=%s, город=%s, delivery=%s",
            application_id,
            peer_id,
            source_cmid,
            city,
            "ok" if result not in {None, "access_error"} else "failed",
        )

    except Exception as exc:
        logger.exception("Ошибка обработки возможной заявки: %s", exc)


def _extract_application_text_from_message(message: dict[str, Any], depth: int = 0) -> str:
    """Берёт текст не только сверху, но и из reply/forwarded сообщений."""
    if not isinstance(message, dict) or depth > 5:
        return ""

    parts: list[str] = []

    own_text = str(message.get("text") or "").strip()
    if own_text:
        parts.append(own_text)

    reply = message.get("reply_message")
    if isinstance(reply, dict):
        reply_text = _extract_application_text_from_message(reply, depth + 1)
        if reply_text:
            parts.append(reply_text)

    forwarded = message.get("fwd_messages") or message.get("forwarded_messages") or []
    if isinstance(forwarded, list):
        for nested in forwarded[:50]:
            if not isinstance(nested, dict):
                continue
            nested_text = _extract_application_text_from_message(nested, depth + 1)
            if nested_text:
                parts.append(nested_text)

    # Убираем только буквальные повторения внутри одного события.
    result: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = re.sub(r"\s+", " ", part).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(part.strip())

    return "\n\n".join(result).strip()


def _extract_application_attachments_from_message(
    message: dict[str, Any],
    depth: int = 0,
) -> list[str]:
    """Собирает фото/видео/документы сверху, из reply и forward."""
    if not isinstance(message, dict) or depth > 5:
        return []

    refs: list[str] = []

    try:
        refs.extend(extract_attachment_references(message))
    except Exception:
        pass

    reply = message.get("reply_message")
    if isinstance(reply, dict):
        refs.extend(_extract_application_attachments_from_message(reply, depth + 1))

    forwarded = message.get("fwd_messages") or message.get("forwarded_messages") or []
    if isinstance(forwarded, list):
        for nested in forwarded[:50]:
            if isinstance(nested, dict):
                refs.extend(_extract_application_attachments_from_message(nested, depth + 1))

    result: list[str] = []
    seen: set[str] = set()
    for ref in refs:
        value = str(ref or "").strip()
        if value and value not in seen:
            seen.add(value)
            result.append(value)

    return result[:10]


def _capture_message_as_application(message: dict[str, Any]) -> None:
    """Единая точка захвата как Long Poll, так и recovery history."""
    if not isinstance(message, dict):
        return

    try:
        peer_id = int(message.get("peer_id") or 0)
    except (TypeError, ValueError):
        return

    # Только реальные групповые беседы.
    if peer_id < 2_000_000_000:
        return

    try:
        sender_id = int(
            message.get("from_id")
            or message.get("user_id")
            or message.get("sender_id")
            or 0
        )
    except (TypeError, ValueError):
        sender_id = 0

    if bool(message.get("out")) or _is_own_group_sender(sender_id):
        return

    action = message.get("action")
    if isinstance(action, dict):
        return

    combined_text = _extract_application_text_from_message(message)
    attachments = _extract_application_attachments_from_message(message)

    if not combined_text and not attachments:
        return

    process_possible_application(
        peer_id,
        sender_id,
        combined_text,
        message.get("date"),
        bool(message.get("out", 0)),
        int(message.get("id") or 0),
        int(message.get("conversation_message_id") or 0),
        attachments,
    )


def _load_application_capture_state() -> dict[str, Any]:
    data = read_json(APPLICATION_CAPTURE_STATE_PATH, {"last_cmid": {}})
    if not isinstance(data, dict):
        data = {"last_cmid": {}}
    if not isinstance(data.get("last_cmid"), dict):
        data["last_cmid"] = {}
    return data


def _save_application_capture_state(data: dict[str, Any]) -> None:
    write_json_atomic(APPLICATION_CAPTURE_STATE_PATH, data)


def _recover_messages_by_cmid(
    client: Any,
    peer_id: int,
    start_cmid: int,
    end_cmid: int,
) -> int:
    """Восстанавливает сообщения, которые Long Poll мог пропустить."""
    recovered = 0
    if end_cmid < start_cmid:
        return 0

    current = start_cmid
    while current <= end_cmid:
        ids = list(range(current, min(end_cmid, current + 99) + 1))
        current = ids[-1] + 1

        try:
            response = client.messages.getByConversationMessageId(
                peer_id=int(peer_id),
                conversation_message_ids=",".join(str(value) for value in ids),
            )
        except Exception as exc:
            logger.debug(
                "Recovery заявок: не удалось получить cmid %s-%s для %s: %s",
                ids[0],
                ids[-1],
                peer_id,
                exc,
            )
            continue

        items = response.get("items", []) if isinstance(response, dict) else []
        for message in items:
            if not isinstance(message, dict):
                continue
            _capture_message_as_application(message)
            recovered += 1

    return recovered


def application_recovery_loop() -> None:
    """Периодически дочитывает историю бесед и ловит пропущенные Long Poll события."""
    # Даём основному Long Poll спокойно запуститься.
    time.sleep(8)

    while True:
        try:
            publish_chat = get_applications_chat()
            peers = sorted({
                int(value)
                for value in list(chat_ids)
                if str(value).isdigit()
                and int(value) >= 2_000_000_000
                and int(value) not in {int(admin_chat or 0), int(publish_chat or 0)}
            })

            if not peers:
                time.sleep(APPLICATION_RECOVERY_INTERVAL_SECONDS)
                continue

            client = get_bulk_vk()
            state = _load_application_capture_state()
            cursors: dict[str, Any] = state.setdefault("last_cmid", {})
            changed = False

            for offset in range(0, len(peers), 80):
                batch = peers[offset:offset + 80]
                try:
                    response = client.messages.getConversationsById(
                        peer_ids=",".join(str(value) for value in batch),
                    )
                except Exception as exc:
                    logger.debug("Recovery заявок: getConversationsById: %s", exc)
                    continue

                items = response.get("items", []) if isinstance(response, dict) else []

                for row in items:
                    if not isinstance(row, dict):
                        continue
                    conversation = row.get("conversation") or {}
                    peer = conversation.get("peer") or {}
                    last_message = row.get("last_message") or {}

                    try:
                        peer_id = int(peer.get("id") or last_message.get("peer_id") or 0)
                        latest_cmid = int(last_message.get("conversation_message_id") or 0)
                    except (TypeError, ValueError):
                        continue

                    if not peer_id or not latest_cmid:
                        continue

                    key = str(peer_id)
                    try:
                        previous_cmid = int(cursors.get(key) or 0)
                    except (TypeError, ValueError):
                        previous_cmid = 0

                    # При первом запуске начинаем "сейчас", чтобы не тащить
                    # многомесячную историю сотен чатов.
                    if previous_cmid <= 0:
                        cursors[key] = latest_cmid
                        changed = True
                        continue

                    if latest_cmid <= previous_cmid:
                        continue

                    recovered = _recover_messages_by_cmid(
                        client,
                        peer_id,
                        previous_cmid + 1,
                        latest_cmid,
                    )
                    cursors[key] = latest_cmid
                    changed = True

                    if recovered:
                        logger.info(
                            "Recovery заявок: чат %s, восстановлено сообщений: %s",
                            peer_id,
                            recovered,
                        )

            if changed:
                state["last_cmid"] = cursors
                _save_application_capture_state(state)

            # Повторная доставка тех заявок, которые сохранились,
            # но уведомление в админ-чат временно не ушло.
            with applications_lock:
                data = load_applications()
                retry_items = [
                    dict(item)
                    for item in data.get("items", [])
                    if item.get("status") == "pending"
                    and int(item.get("review_message_id", 0) or 0) == 0
                    and item.get("delivery_status") == "failed"
                ][-30:]

            for item in retry_items:
                attachment_refs = item.get("attachment_refs") or []
                attachment_value = (
                    ",".join(str(x) for x in attachment_refs[:10])
                    if isinstance(attachment_refs, list) and attachment_refs
                    else None
                )
                result = send_message(
                    admin_chat,
                    render_application_text(item),
                    attachment=attachment_value,
                    keyboard=build_application_keyboard(int(item.get("id", 0) or 0)),
                    rate_limit=False,
                    api_client=client,
                )
                if result in {None, "access_error"}:
                    continue

                with applications_lock:
                    latest = load_applications()
                    target = next(
                        (
                            x for x in latest["items"]
                            if int(x.get("id", 0) or 0) == int(item.get("id", 0) or 0)
                        ),
                        None,
                    )
                    if target is not None:
                        target["review_message_id"] = int(result)
                        target["delivery_status"] = "sent"
                        target["delivered_at"] = now_msk_str()
                        save_applications(latest)

        except Exception as exc:
            logger.exception("Ошибка recovery заявок: %s", exc)

        time.sleep(APPLICATION_RECOVERY_INTERVAL_SECONDS)


def handle_application_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    command = str(payload.get("command") or "").lower()
    if command not in {"application_approve", "application_reject"}:
        return False
    event_object = payload.get("_event_object")
    publish_chat = get_applications_chat()
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Недостаточно прав.")
        return True
    try:
        application_id = int(payload.get("application_id") or 0)
    except (TypeError, ValueError):
        return True
    with applications_lock:
        data = load_applications()
        item = next((x for x in data["items"] if int(x.get("id", 0) or 0) == application_id), None)
        if not item or item.get("status") != "pending":
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Заявка уже обработана.")
            return True
        cmid = int(event_object.get("conversation_message_id") or 0) if isinstance(event_object, dict) else 0
        if command == "application_reject":
            item["status"] = "rejected"
            item["processed_at"] = now_msk_str()
            item["processed_by"] = user_id
            save_applications(data)
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Заявка отклонена.")
            if cmid:
                delete_chat_message_by_cmid(chat_id, cmid)
            return True

        if not publish_chat:
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Сначала установите чат командой .устзаявки")
            return True

        published_id = send_message(
            publish_chat,
            render_application_text(item, approved=True),
            rate_limit=False,
        )
        if published_id is None or published_id == "access_error":
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Не удалось опубликовать объявление.")
            return True
        item["status"] = "published"
        item["processed_at"] = now_msk_str()
        item["processed_by"] = user_id
        item["published_message_id"] = int(published_id)
        item["published_peer_id"] = int(publish_chat)
        item["published_ts"] = time.time()
        item["expires_ts"] = time.time() + APPLICATION_LIFETIME_SECONDS
        save_applications(data)
        threading.Thread(
            target=notify_job_subscribers_for_application,
            args=(dict(item),),
            daemon=True,
        ).start()
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Опубликовано на 6 часов.")
        if cmid:
            delete_chat_message_by_cmid(chat_id, cmid)
        return True


def applications_cleanup_loop() -> None:
    while True:
        try:
            now_ts = time.time()
            changed = False
            with applications_lock:
                data = load_applications()
                for item in data["items"]:
                    if item.get("status") != "published":
                        continue
                    expires_ts = float(item.get("expires_ts", 0) or 0)
                    if not expires_ts or expires_ts > now_ts:
                        continue
                    peer_id = int(item.get("published_peer_id", 0) or 0) or get_applications_chat()
                    message_id = int(item.get("published_message_id", 0) or 0)
                    if peer_id and message_id:
                        delete_chat_message(peer_id, message_id)
                    item["status"] = "expired"
                    item["expired_at"] = now_msk_str()
                    changed = True
                # Храним обработанные записи 30 дней, чтобы файл не рос бесконечно.
                month_ago = now_ts - 30 * 24 * 60 * 60
                data["items"] = [
                    item for item in data["items"]
                    if item.get("status") in {"pending", "published"} or float(item.get("created_ts", now_ts) or now_ts) >= month_ago
                ]
                if changed:
                    save_applications(data)
        except Exception as exc:
            logger.exception(f"Ошибка автоудаления объявлений: {exc}")
        time.sleep(30)

def _is_duplicate_event(message: dict[str, Any]) -> bool:
    event_id = str(message.get("id") or message.get("conversation_message_id") or "")
    if not event_id:
        return False
    now = time.time()
    key = f"{message.get('peer_id', '')}:{event_id}"
    with _processed_events_lock:
        if key in _processed_events:
            return True
        _processed_events[key] = now
        while len(_processed_events) > 500:
            _processed_events.popitem(last=False)
    return False


def generate_random_id() -> int:
    return int(time.time() * 1000000) % (2**31 - 1)


def wait_service_send_cooldown() -> None:
    global _last_service_send_at
    # Раньше сюда попадала общая пауза 1.5 секунды. Из-за этого сам Long Poll
    # ждал перед каждым ответом и казалось, что бот умер во время рассылки.
    # Интерактивные ответы получают отдельный короткий лимит, а массовые
    # отправки регулируются wait_bulk_send_slot().
    configured = max(0.0, float(service_send_cooldown_sec or 0))
    delay = min(configured, INTERACTIVE_SEND_COOLDOWN_CAP)
    if delay <= 0:
        return
    with _service_send_lock:
        now = time.monotonic()
        wait_for = _last_service_send_at + delay - now
        if wait_for > 0:
            time.sleep(wait_for)
        _last_service_send_at = time.monotonic()


def load_json_file(path: str | Path, default: Any) -> Any:
    return read_json(path, default)


def load_users() -> dict[str, Any]:
    return read_json(USERS_DB_PATH, {})


def save_users(data: dict[str, Any]) -> None:
    write_json_atomic(USERS_DB_PATH, data)


def mark_pm_activated(user_id: int) -> None:
    """Запоминает пользователя ЛС без постоянной перезаписи всей базы."""
    try:
        normalized_user_id = int(user_id)
    except (TypeError, ValueError):
        return
    if normalized_user_id <= 0:
        return

    now_text = now_msk_str()
    with _users_db_lock:
        users = load_users()
        user = users.setdefault(
            str(normalized_user_id),
            {
                "role": "user",
                "osn_photo_count": 0,
                "osn_text_count": 0,
                "total_messages": 0,
                "last_message": "",
                "stats": {},
            },
        )
        already_active = bool(user.get("pm_activated"))
        last_activity = str(user.get("pm_last_activity") or "")
        if already_active and last_activity:
            try:
                last_dt = datetime.strptime(last_activity, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
                if (now_msk() - last_dt).total_seconds() < 300:
                    return
            except (TypeError, ValueError):
                pass
        if not user.get("pm_activated_at"):
            user["pm_activated_at"] = now_text
        user["pm_activated"] = True
        user["pm_last_activity"] = now_text
        save_users(users)


def get_activated_pm_user_ids() -> list[int]:
    """Возвращает пользователей ЛС без тяжёлой повторной синхронизации.

    Старые диалоги VK подхватываются не чаще одного раза в 6 часов и затем
    сохраняются в users_db.json одним атомарным обновлением. Раньше полный
    список диалогов запрашивался при каждом .инфозов, а база переписывалась
    отдельно после каждой успешной отправки.
    """
    with _users_db_lock:
        users = load_users()
        result = {
            int(uid)
            for uid, data in users.items()
            if str(uid).isdigit()
            and int(uid) > 0
            and isinstance(data, dict)
            and bool(data.get("pm_activated"))
        }

    try:
        last_sync_ts = float(runtime_data.get("pm_dialog_sync_ts", 0) or 0)
    except (TypeError, ValueError):
        last_sync_ts = 0.0
    if time.time() - last_sync_ts < PM_DIALOG_SYNC_TTL_SECONDS:
        return sorted(result)

    discovered: set[int] = set()
    try:
        client = get_bulk_vk()
        offset = 0
        page_size = 200
        while True:
            response = client.messages.getConversations(offset=offset, count=page_size, filter="all")
            items = response.get("items", []) if isinstance(response, dict) else []
            for item in items:
                conversation = (item or {}).get("conversation") or {}
                peer = conversation.get("peer") or {}
                if str(peer.get("type") or "") != "user":
                    continue
                try:
                    peer_id = int(peer.get("id") or 0)
                except (TypeError, ValueError):
                    peer_id = 0
                if peer_id > 0:
                    discovered.add(peer_id)
            offset += len(items)
            total = int(response.get("count", 0) or 0) if isinstance(response, dict) else 0
            if not items or offset >= total:
                break
    except Exception as exc:
        logger.warning(f"Не удалось полностью обновить список пользователей ЛС: {exc}")
        return sorted(result)

    if discovered:
        now_text = now_msk_str()
        with _users_db_lock:
            latest_users = load_users()
            changed = False
            for peer_id in discovered:
                key = str(peer_id)
                user = latest_users.setdefault(
                    key,
                    {
                        "role": "user",
                        "osn_photo_count": 0,
                        "osn_text_count": 0,
                        "total_messages": 0,
                        "last_message": "",
                        "stats": {},
                    },
                )
                if not user.get("pm_activated"):
                    user["pm_activated"] = True
                    user["pm_activated_at"] = user.get("pm_activated_at") or now_text
                    changed = True
            if changed:
                save_users(latest_users)
        result.update(discovered)

    with _runtime_data_lock:
        runtime_data["pm_dialog_sync_ts"] = time.time()
        runtime_data["pm_dialog_sync_at"] = now_msk_str()
        save_runtime_data()
    return sorted(result)


def get_role(user_id: int) -> str | None:
    if user_id in dev_ids:
        return "dev"
    if user_id in admin_ids:
        return "admin"
    users = load_users()
    return users.get(str(user_id), {}).get("role")


def has_permission(user_id: int, level: str) -> bool:
    role = get_role(user_id)
    if level == "dev":
        return role == "dev"
    if level == "admin":
        return role in {"admin", "dev"}
    return False


def update_user_stats(user_id: int, action: str) -> None:
    with _users_db_lock:
        users = load_users()
        user = users.setdefault(
            str(user_id),
            {
                "role": "user",
                "osn_photo_count": 0,
                "osn_text_count": 0,
                "total_messages": 0,
                "last_message": "",
                "stats": {},
            },
        )
        stats = user.setdefault("stats", {})
        stats[action] = int(stats.get(action, 0) or 0) + 1
        stats["last_activity"] = now_msk_str()
        if action == "osn_photo":
            user["osn_photo_count"] = int(user.get("osn_photo_count", 0) or 0) + 1
        elif action == "osn_text":
            user["osn_text_count"] = int(user.get("osn_text_count", 0) or 0) + 1
        elif action == "command":
            user["total_messages"] = int(user.get("total_messages", 0) or 0) + 1
        user["last_message"] = now_msk_str()
        save_users(users)


def save_ads() -> None:
    order_service.save_ads(ads_data)


def save_orders() -> None:
    order_service.save_orders(orders_data)


def append_income(user_id: int, amount: int, description: str) -> None:
    incomes = load_json_file(INCOMES_PATH, [])
    incomes.append({"user_id": user_id, "amount": amount, "description": description, "created_at": now_msk_str()})
    write_json_atomic(INCOMES_PATH, incomes)


def get_chat_inviter(chat_id: int) -> int | None:
    log = load_json_file(INVITES_LOG_PATH, [])
    for entry in reversed(log):
        if entry.get("chat_id") == chat_id and entry.get("user_id"):
            return int(entry["user_id"])
    return None


def mark_invite_confirmed(chat_id: int) -> None:
    log = load_json_file(INVITES_LOG_PATH, [])
    changed = False
    for entry in log:
        if entry.get("chat_id") == chat_id and not entry.get("confirmed"):
            entry["confirmed"] = True
            entry["confirmed_at"] = now_msk_str()
            changed = True
    if changed:
        write_json_atomic(INVITES_LOG_PATH, log)


def is_invite_confirmed(chat_id: int) -> bool:
    log = load_json_file(INVITES_LOG_PATH, [])
    for entry in log:
        if entry.get("chat_id") == chat_id and entry.get("confirmed"):
            return True
    return False


def log_invite(user_id: int, chat_id: int, chat_title: str) -> None:
    log = load_json_file(INVITES_LOG_PATH, [])
    log.append({
        "user_id": user_id,
        "chat_id": chat_id,
        "chat_title": chat_title or "Без названия",
        "created_at": now_msk_str(),
    })
    write_json_atomic(INVITES_LOG_PATH, log)


def save_runtime_data() -> None:
    # Несколько фоновых задач могут завершиться одновременно. Сохраняем data.json
    # под одним RLock, чтобы рассылка не перетёрла изменения административной команды.
    with _runtime_data_lock:
        runtime_data["chat_ids"] = list(chat_ids)
        runtime_data["admin_chat"] = admin_chat
        runtime_data["message_text"] = message_text
        runtime_data["photo_path"] = message_photo_path
        runtime_data["photo_attachment"] = message_photo_attachment
        runtime_data["main_button_title"] = main_button_title
        runtime_data["main_button_link"] = main_button_link
        runtime_data["additional_texts"] = runtime_data.get("additional_texts", [])
        runtime_data["additional_photos_by_text"] = runtime_data.get("additional_photos_by_text", {})
        runtime_data["chat_registry"] = runtime_data.get("chat_registry", {})
        runtime_data["service_send_cooldown_sec"] = service_send_cooldown_sec
        runtime_data["applications_chat"] = runtime_data.get("applications_chat")
        write_json_atomic(DATA_PATH, runtime_data)


def get_chat_registry() -> dict[str, Any]:
    value = runtime_data.get("chat_registry", {})
    return value if isinstance(value, dict) else {}


def _fetch_all_conversation_titles() -> dict[int, str]:
    """Получить названия всех бесед бота одним пакетом (пагинация)."""
    client = get_bulk_vk()
    titles: dict[int, str] = {}
    offset = 0
    count = 200
    total_items = 0
    while True:
        try:
            result = client.messages.getConversations(offset=offset, count=count, extended=1)
        except Exception as exc:
            logger.warning(f"getConversations offset={offset}: {exc}")
            break
        items = result.get("items", [])
        total_items += len(items)
        for item in items:
            conv = item.get("conversation", {})
            peer = conv.get("peer", {})
            peer_id = peer.get("id")
            title = peer.get("title")
            ptype = peer.get("type")
            if peer_id and ptype == "chat":
                if title:
                    titles[peer_id] = title
        if len(items) < count:
            break
        offset += count
    logger.info(f"getConversations: итого {total_items} бесед, {len(titles)} с названиями")

    # Пробуем пакетный getConversationsById для чатов без названий из registry
    registry = get_chat_registry()
    need_title = [int(k) for k, v in registry.items() if v.get("chat_id") and not v.get("title")]
    if need_title:
        # Пакетами по 100
        for i in range(0, len(need_title), 100):
            batch = need_title[i:i+100]
            try:
                r = client.messages.getConversationsById(peer_ids=",".join(str(x) for x in batch))
                for item in r.get("items", []):
                    cs = (item.get("conversation") or {}).get("chat_settings") or {}
                    pid = (item.get("conversation") or {}).get("peer", {}).get("id")
                    t = cs.get("title")
                    if pid and t:
                        titles[pid] = t
            except Exception as exc:
                logger.warning(f"getConversationsById batch: {exc}")
    return titles


def refresh_chat_registry_entry(chat_id: int) -> dict[str, Any]:
    """Обновляет один чат одним запросом и только в фоновом потоке."""
    client = get_bulk_vk()
    title = None
    link = None
    try:
        conversation = client.messages.getConversationsById(peer_ids=chat_id).get("items", [])
        if conversation:
            conversation_data = (conversation[0] or {}).get("conversation") or {}
            peer_data = conversation_data.get("peer") or {}
            chat_settings = conversation_data.get("chat_settings") or {}
            title = chat_settings.get("title") or peer_data.get("title")
            link = chat_settings.get("invite_link")
    except Exception as exc:
        logger.debug("Chat %s: не удалось обновить метаданные: %s", chat_id, exc)

    with _runtime_data_lock:
        registry = get_chat_registry()
        key = str(chat_id)
        entry = dict(registry.get(key) or {})
        entry["chat_id"] = chat_id
        entry["updated_at"] = now_msk_str()
        entry["updated_ts"] = time.time()
        if title:
            entry["title"] = title
        if link:
            entry["link"] = link
        registry[key] = entry
        runtime_data["chat_registry"] = registry
        save_runtime_data()
    if title:
        _update_invite_log_title(chat_id, title)
    return entry


def request_chat_registry_refresh(chat_id: int, *, force: bool = False) -> None:
    """Ставит лёгкое обновление метаданных в ограниченный пул, без ожидания Long Poll."""
    registry = get_chat_registry()
    entry = registry.get(str(chat_id)) if isinstance(registry, dict) else None
    try:
        updated_ts = float((entry or {}).get("updated_ts", 0) or 0)
    except (TypeError, ValueError):
        updated_ts = 0.0
    if not force and updated_ts and time.time() - updated_ts < CHAT_REGISTRY_REFRESH_TTL_SECONDS:
        return
    with _chat_refresh_lock:
        if chat_id in _chat_refresh_inflight:
            return
        _chat_refresh_inflight.add(chat_id)

    def _refresh() -> None:
        try:
            refresh_chat_registry_entry(chat_id)
        finally:
            with _chat_refresh_lock:
                _chat_refresh_inflight.discard(chat_id)

    _background_executor.submit(_refresh)


def _update_invite_log_title(chat_id: int, title: str) -> None:
    log = load_json_file(INVITES_LOG_PATH, [])
    changed = False
    for entry in log:
        if entry.get("chat_id") == chat_id and entry.get("chat_title") in {"Без названия", ""}:
            entry["chat_title"] = title
            changed = True
    if changed:
        write_json_atomic(INVITES_LOG_PATH, log)


def refresh_all_chat_titles() -> tuple[int, int]:
    registry = get_chat_registry()
    updated = 0
    failed = 0

    # Один запрос — все беседы бота
    all_titles = _fetch_all_conversation_titles()
    logger.info(f"getConversations вернул {len(all_titles)} чатов с названиями")

    for key, entry in list(registry.items()):
        cid = entry.get("chat_id")
        if not cid:
            continue
        title = all_titles.get(cid)
        if title and title != entry.get("title"):
            entry["title"] = title
            _update_invite_log_title(cid, title)
            updated += 1
    runtime_data["chat_registry"] = registry
    save_runtime_data()
    return updated, failed


def start_chat_announcement(chat_id: int) -> None:
    def _run() -> None:
        try:
            items = build_vk_full_broadcast_items()
            result = send_broadcast_to_chat(chat_id, items)
            if result != "access_error":
                mark_invite_confirmed(chat_id)
        except Exception as exc:
            logger.error(f"Ошибка приветственной отправки в чат {chat_id}: {exc}")

    _background_executor.submit(_run)


def ensure_chat_registered(chat_id: int, *, announce: bool = False) -> bool:
    """Регистрирует чат без сетевых запросов в основном цикле Long Poll."""
    current_admin = runtime_data.get("admin_chat", MESSAGE_CONFIG.get("admin_chat"))
    if current_admin is not None and chat_id == current_admin:
        return False
    if not (len(str(chat_id)) == 10 and str(chat_id).startswith("2")):
        return False

    with _chat_ids_lock:
        is_new = chat_id not in chat_ids
        if is_new:
            chat_ids.append(chat_id)

    registry = get_chat_registry()
    entry = dict(registry.get(str(chat_id)) or {}) if isinstance(registry, dict) else {}
    needs_metadata = is_new or not entry.get("title") or not entry.get("updated_ts")
    if is_new:
        with _runtime_data_lock:
            registry = get_chat_registry()
            registry[str(chat_id)] = {
                **dict(registry.get(str(chat_id)) or {}),
                "chat_id": chat_id,
                "registered_at": now_msk_str(),
            }
            runtime_data["chat_registry"] = registry
            save_runtime_data()
    if needs_metadata:
        request_chat_registry_refresh(chat_id, force=is_new)
    if announce and is_new:
        start_chat_announcement(chat_id)
    return is_new


def remove_chat_from_broadcast_list(chat_id: int, reason: str) -> None:
    if chat_id == admin_chat:
        return
    removed = False
    with _chat_ids_lock:
        if chat_id in chat_ids:
            chat_ids.remove(chat_id)
            removed = True
    if removed:
        save_runtime_data()
        logger.info(f"Chat {chat_id}: удалён из списка рассылки ({reason})")

# main.py, после инициализации admin_chat и chat_ids
admin_chat = runtime_data.get("admin_chat", MESSAGE_CONFIG.get("admin_chat"))
chat_ids = runtime_data.get("chat_ids", MESSAGE_CONFIG.get("chat_ids", []))

# Удаляем админ-чат из списка рассылки, если он там оказался
if admin_chat is not None and admin_chat in chat_ids:
    chat_ids.remove(admin_chat)
    save_runtime_data()
    logger.info(f"Удалён админ-чат {admin_chat} из списка рассылки при старте")


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
    except (TypeError, ValueError):
        return None


def is_user_mirror_ad_key(key: str, ad_data: dict[str, Any]) -> bool:
    user_id = ad_data.get("user_id")
    return str(key).isdigit() and user_id is not None and str(key) == str(user_id)


def get_help_text(role: str | None) -> str:
    user_commands = (
        "📋 Основные команды:\n"
        "🔹 .пинг — проверить, работает ли бот\n"
        "🔹 .стата — посмотреть свою статистику\n"
    )

    admin_commands = (
        "🔸 Административные команды:\n"
        "🔹 .редтекст [номер] [текст] — редактировать дополнительный текст\n"
        "🔹 .рассылка — запустить рассылку\n"
        "🔹 .стопзаказ <номер> — приостановить заказ в рассылке\n"
        "🔹 .стартзаказ <номер> — вернуть заказ в рассылку\n"
        "🔹 .отпрзаказ <номер> — разово отправить только этот заказ во все чаты\n"
        "🔹 .список — показать список чатов рассылки\n"
        "🔹 /поискчаты — до 100 новых бесед; поиск по всем городам РФ, регионам и сокращениям\n"
        "🔹 /поискчаты 80 — только беседы, где указано от 80 участников\n"
        "🔹 /поискчаты Работа Новосибирск — искать на сайте точно по запросу «Работа Новосибирск»\n"
        "🔹 /поискчаты Работа Новосибирск 80 — точный запрос + от 80 участников\n"
        "🔹 .обновитьчаты — обновить названия и ссылки чатов\n"
        "🔹 .ид — узнать ID текущего чата\n"
        "🔹 .инфо — показать текущие настройки\n"
        "🔹 .хелп — показать это сообщение\n"
        "🔹 .тест — отправить тестовое сообщение\n"
        "🔹 .списокрекламы — показать список действующих заказов рекламы\n"
        "🔹 .допсписок — показать список дополнительных текстов\n"
        "🔹 .заказы — показать заявки на модерации\n"
        "🔹 .историязаказов — показать обработанные заказы\n"
        "🔹 .уст — добавить текущий чат в рассылку\n"
        "🔹 .инфочат — получить информацию о чате\n"
        "🔹 .добид [число] — добавить указанное количество ID в список\n"
        "🔹 .делид [число] — удалить указанное количество ID с конца списка\n"
        "🔹 .добфото [номер] — добавить фото к дополнительному тексту\n"
        "🔹 .удфото [номер] — удалить фото у дополнительного текста\n"
        "🔹 .создатькомпанию <Название> — создать внутреннюю компанию\n"
        "🔹 .компании — показать внутренний список компаний\n"
        "🔹 .удкомпания <номер/название> — удалить компанию\n"
        "🔹 .добработа — выбрать компанию и добавить работу в каталог ЛС\n"
        "🔹 .измработа <номер> — изменить кнопку, место, формат, текст и фото работы\n"
        "🔹 .редработа <номер> — то же самое (старое название команды)\n"
        "🔹 .редрабфото <номер> + фото — быстро заменить фото вакансии без полного редактирования\n"
        "🔹 .удработа <номер> — удалить неактуальную работу\n"
        "🔹 .работы — показать каталог работ для ЛС\n"
        "🔹 .инфозов — уведомить всех активировавших ЛС о новых вакансиях\n"
        "🔹 .добвакрассылка [текст] — создать/обновить компактный блок вакансий (можно с фото)\n"
        "🔹 .редвак — открыть меню редактирования компактного блока вакансий\n"
        "🔹 .вакрассылка — показать превью блока вакансий\n"
        "🔹 .удвакрассылка — убрать блок вакансий из общей рассылки\n"
    )

    dev_commands = (
        "🔧 Команды разработчика:\n"
        "🔹 .админ [id/@] — выдать или снять права администратора\n"
        "🔹 .разраб [id/@/ответ] — выдать или снять права разработчика\n"
        "🔹 .настройки [cd_min|interval_sec|service_send_cooldown_sec] [число] — изменить тайминги\n"
        "🔹 .редоснтекст [текст] — изменить основной текст рассылки\n"
        "🔹 .редоснфото — изменить основное фото или видео рассылки\n"
        "🔹 .редоснкнопка — изменить название или ссылку кнопки основного сообщения\n"
        "🔹 .gzov — разослать только основное сообщение бота во все чаты\n"
        "🔹 .стафф — показать состав персонала\n"
        "🔹 .админчат — установить текущий чат как административный\n"
        "🔹 .доходы — посмотреть статистику доходов\n"
        "🔹 .добзаказ @username [дни] [текст] — добавить заказ вручную\n"
        "🔹 .удзаказ @username — удалить активный заказ пользователя\n"
        "🔹 .измзаказ <order_id|номер> <поле> <значение> — изменить заказ\n"
        "🔹 .оффрассылка [id] — удалить чат из рассылки (без id — текущий чат)\n"
        "🔹 .отправить [chat_id] [текст] — отправить сообщение в чат (с фото)\n"
        "🔹 .логиприглашения — лог приглашений бота в чаты\n"
        "🔹 .логиприглашения @user — приглашения от конкретного пользователя\n"
        "🔹 .обновитназвания — обновить названия всех чатов\n"
"🔹 .базачаты [страница] — все чаты, найденные поиском\n"
        "🔹 .архивчаты [страница] — полный архив всех найденных чатов за всё время\n"
"🔹 .статакнопки — выбрать кнопку рассылки и посмотреть статистику\n"
        "🔹 .статистиканажатий [страница] — журнал нажатий с датой и временем\n"
        "🔹 .название [chat_id] [текст] — задать название чата вручную\n"
    )

    full_text = user_commands
    if role in {"admin", "dev"}:
        full_text += "\n\n" + admin_commands
    if role == "dev":
        full_text += "\n\n" + dev_commands
    return full_text


def get_user_display_name(user_id: int) -> str:
    name = _resolve_vk_sender_name(int(user_id))
    return name or f"Пользователь {user_id}"


def render_user_stats(user_id: int) -> str:
    users = load_users()
    user = users.get(str(user_id), {})
    stats = user.get("stats", {})
    total_commands = int(user.get("total_messages", stats.get("command", 0)) or 0)
    name = get_user_display_name(user_id)
    return (
        "📊 Ваша статистика\n\n"
        f"🔹 Имя: [id{user_id}|{name}]\n"
        f"🔹 Команд использовано: {total_commands}\n"
        f"🔹 Изменений основного фото: {int(user.get('osn_photo_count', 0) or 0)}\n"
        f"🔹 Изменений основного текста: {int(user.get('osn_text_count', 0) or 0)}\n"
        f"🔹 Последняя активность: {stats.get('last_activity', 'нет данных')}"
    )


def render_user_stats_detailed(user_id: int) -> str:
    users = load_users()
    user = users.get(str(user_id), {})
    stats = user.get("stats", {})
    total_commands = int(user.get("total_messages", stats.get("command", 0)) or 0)
    role_names = {"user": "Пользователь", "admin": "Администратор", "dev": "Разработчик"}
    role_display = role_names.get(user.get("role", "user"), "Пользователь")
    log = load_json_file(INVITES_LOG_PATH, [])
    invite_count = len([e for e in log if int(e.get("user_id", 0) or 0) == user_id])
    # Рабочее = чат есть в списке рассылки (бот может туда писать)
    working_invites = len([e for e in log if int(e.get("user_id", 0) or 0) == user_id and int(e.get("chat_id", 0) or 0) in chat_ids])
    last_activity = stats.get("last_activity", "")
    name = get_user_display_name(user_id)
    return (
        f"👤 Информация о пользователе:\n\n"
        f"🔹 Имя: [id{user_id}|{name}]\n"
        f"🔹 Роль: {role_display}\n"
        f"🔹 Изменения текста/фото: {int(user.get('osn_text_count', 0) or 0) + int(user.get('osn_photo_count', 0) or 0)}\n"
        f"🔹 Всего сообщений для бота: {total_commands}\n"
        f"🔹 Пригласил бота в чаты: {invite_count} раз (из них рабочих: {working_invites})\n"
        f"🔹 Последняя активность: {last_activity or 'нет данных'}"
    )


def render_staff_detailed() -> str:
    users = load_users()
    devs = []
    admins = []
    for owner_id in sorted(OWNER_IDS):
        try:
            owner_info = vk.users.get(user_ids=owner_id)[0]
            owner_name = f"{owner_info['first_name']} {owner_info['last_name']}"
            devs.append(f"• [id{owner_id}|{owner_name}]")
        except Exception:
            devs.append(f"• [id{owner_id}|Разработчик]")


    for uid, data in users.items():
        if data.get("role") == "admin":
            try:
                info = vk.users.get(user_ids=int(uid))[0]
                name = f"{info['first_name']} {info['last_name']}"
                admins.append(f"• [id{uid}|{name}]")
            except Exception:
                admins.append(f"• [id{uid}|Администратор]")

    return "🔧 Список персонала бота:\n\nРазработчик:\n" + "\n".join(devs) + "\n\nАдминистраторы:\n" + ("\n".join(admins) if admins else "Нет назначенных администраторов")


def render_runtime_info() -> str:
    return (
        "ℹ️ Текущие настройки\n\n"
        f"Чатов в рассылке: {len(chat_ids)}\n"
        f"Админ-чат: {admin_chat or 'не задан'}\n"
        f"Интервал внутри рассылки в одном чате: {interval_sec}\n"
        f"КД обычных сообщений: {service_send_cooldown_sec}\n"
        "Режим рассылки: сразу во все чаты\n"
        f"Авторассылка каждые: {cd_min} мин.\n"
        f"Основной текст: {len(message_text or '')} символов"
    )


def render_runtime_info_legacy() -> str:
    additional_texts = get_additional_texts()
    info_parts = [
        "📊 ИНФОРМАЦИЯ О НАСТРОЙКАХ",
        "",
        f"⏱️ Интервал между рассылками: *{cd_min}* минут",
        f"⚡ Интервал внутри рассылки в одном чате: *{interval_sec}* секунд",
        f"🛡️ КД обычных сообщений: *{service_send_cooldown_sec}* секунд",
        "🚀 Режим рассылки: сразу во все чаты",
        "",
        "📝 ТЕКСТ РАССЫЛКИ:",
        "" if not (message_text or "").strip() else message_text,
        "",
    ]
    if additional_texts:
        info_parts.append("📎 ДОПОЛНИТЕЛЬНЫЕ ТЕКСТЫ:")
        for i, add_text in enumerate(additional_texts, 1):
            if add_text.strip():
                info_parts.append(f"{i}. {add_text.strip()}")
        info_parts.append("")
    return "\n".join(info_parts)


def save_config_value(name: str, value_repr: str) -> None:
    config_path = BASE_DIR / "config.py"
    lines = config_path.read_text(encoding="utf-8").splitlines()
    updated = False
    new_lines: list[str] = []
    for line in lines:
        if line.startswith(f"{name} ="):
            new_lines.append(f"{name} = {value_repr}")
            updated = True
        else:
            new_lines.append(line)
    if updated:
        config_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def render_staff() -> str:
    users = load_users()
    dynamic_admins = [int(uid) for uid, data in users.items() if data.get("role") == "admin"]
    dynamic_devs = [int(uid) for uid, data in users.items() if data.get("role") == "dev"]
    all_admins = sorted(set(admin_ids + dynamic_admins))
    all_devs = sorted(set(dev_ids + dynamic_devs))
    admin_list = ", ".join(f"id{item}" for item in all_admins) or "нет"
    dev_list = ", ".join(f"id{item}" for item in all_devs) or "нет"
    return f"👥 Состав персонала\n\nАдминистраторы: {admin_list}\nРазработчики: {dev_list}"


def get_additional_texts() -> list[str]:
    value = runtime_data.get("additional_texts", [])
    return value if isinstance(value, list) else []


def get_additional_photos() -> dict[str, list[str]]:
    value = runtime_data.get("additional_photos_by_text", {})
    return value if isinstance(value, dict) else {}


def save_additional_texts(texts: list[str], photos_map: dict[str, list[str]] | None = None) -> None:
    runtime_data["additional_texts"] = texts
    if photos_map is not None:
        runtime_data["additional_photos_by_text"] = photos_map
    save_runtime_data()


def send_message(
    chat_id: int,
    text: str,
    attachment: str | None = None,
    keyboard: str | None = None,
    *,
    rate_limit: bool = True,
    api_client: Any | None = None,
) -> Any:
    # Основной HTTP-клиент принадлежит только потоку Long Poll. Любой фоновый
    # поток автоматически получает отдельную thread-local сессию, даже если
    # вызывающий код не передал api_client явно.
    use_primary_client = api_client is None and threading.get_ident() == _main_vk_thread_ident
    client = vk if use_primary_client else (api_client or get_bulk_vk())
    if rate_limit and use_primary_client:
        wait_service_send_cooldown()
    params = {"peer_id": chat_id, "message": text or " ", "random_id": generate_random_id()}
    if attachment:
        params["attachment"] = attachment
    if keyboard:
        params["keyboard"] = keyboard
    try:
        return client.messages.send(**params)
    except Exception as exc:
        error_text = str(exc)
        error_lower = error_text.lower()
        # Для видео НИКОГДА не отправляем текст повторно без вложения.
        # Иначе визуально кажется, что рассылка успешна, хотя ролик исчез.
        is_video_attachment = isinstance(attachment, str) and attachment.startswith("video")

        if (
            attachment
            and not is_video_attachment
            and "attachment" in error_lower
            and ("invalid" in error_lower or "not found" in error_lower)
        ):
            logger.warning(
                "Вложение не принято VK для чата %s, отправляю текст без него: %s",
                chat_id,
                exc,
            )
            params.pop("attachment", None)
            try:
                return client.messages.send(**params)
            except Exception as retry_exc:
                logger.error(
                    "Повторная отправка без вложения в чат %s не удалась: %s",
                    chat_id,
                    retry_exc,
                )
                return None

        if is_video_attachment:
            logger.error(
                "VK не принял ОСНОВНОЕ VIDEO-вложение %s в чат %s: %s",
                attachment,
                chat_id,
                exc,
            )
        if "the user was kicked out of the conversation" in error_lower:
            remove_chat_from_broadcast_list(chat_id, "участник исключён из беседы")
            return None
        if "you are restricted to write to a chat" in error_lower or "code 983" in error_lower:
            remove_chat_from_broadcast_list(chat_id, "ограничение на запись")
            return None
        if "ошибка доступа к чату" in error_lower or "you don't have access to this chat" in error_lower or "access denied" in error_lower:
            remove_chat_from_broadcast_list(chat_id, "ошибка доступа к чату")
            logger.warning(f"Chat {chat_id}: ошибка доступа. Чат удалён из рассылки.")
            return "access_error"
        if "chat not found" in error_lower or "peer_id" in error_lower:
            remove_chat_from_broadcast_list(chat_id, "чат не найден")
            return None
        logger.error(f"Ошибка отправки в чат {chat_id}: {exc}")
        return None


def send_check_to_chat(chat_id: int, order_id: str, order: dict[str, Any] | None) -> None:
    attachment = check_attachment_reference_from_order(order)
    title = check_file_title_from_order(order)
    if attachment:
        result = send_message(chat_id, f"📎 Чек по заявке {order_id}\n{title}", attachment=attachment)
        if result is not None:
            return
    link = check_file_link_from_order(order)
    if link:
        send_message(chat_id, f"📎 Чек по заявке {order_id}\n{title}\n{link}")
    else:
        send_message(chat_id, "❌ Не удалось отправить файл чека. Попросите пользователя отправить чек фото или документом ещё раз.")


def send_long_message(chat_id: int, text: str, *, chunk_size: int = 3500) -> None:
    if not text:
        send_message(chat_id, " ")
        return
    if len(text) <= chunk_size:
        send_message(chat_id, text)
        return

    current_chunk: list[str] = []
    current_len = 0
    for line in text.splitlines():
        line_len = len(line) + 1
        if current_chunk and current_len + line_len > chunk_size:
            send_message(chat_id, "\n".join(current_chunk))
            current_chunk = [line]
            current_len = line_len
        else:
            current_chunk.append(line)
            current_len += line_len

    if current_chunk:
        send_message(chat_id, "\n".join(current_chunk))


def render_chat_list_text() -> str:
    registry = get_chat_registry()
    target_chats = [cid for cid in chat_ids if cid != admin_chat]
    lines_out = []
    for index, cid in enumerate(target_chats, start=1):
        entry = registry.get(str(cid), {})
        title = entry.get("title") or "Название недоступно"
        link = entry.get("link") or "Ссылка недоступна"
        lines_out.append(
            f"{index}. ID: {cid}\n"
            f"   Название: {title}\n"
            f"   Ссылка: {link}"
        )
    chat_list = "\n\n".join(lines_out) if lines_out else "Список чатов пуст."
    return f"📋 Чаты в рассылке: {len(target_chats)}\n\n{chat_list}"


def build_extend_specific_keyboard(item_key: str, is_main: bool) -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "💰 Продлить рекламу",
        color=VkKeyboardColor.POSITIVE,
        payload={"command": "start_extend", "item_key": item_key},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📨 Мои сообщения",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": ".сообщение"},
    )
    return keyboard.get_keyboard()


def upload_message_photo() -> str | None:
    with _media_upload_lock:
        return _upload_message_photo_unlocked()


def _upload_message_photo_unlocked() -> str | None:
    global uploaded_message_photo, uploaded_message_photo_at

    if uploaded_message_photo and (time.time() - uploaded_message_photo_at < UPLOADED_PHOTO_MAX_AGE):
        return uploaded_message_photo

    uploaded_message_photo = None
    uploaded_message_photo_at = 0.0

    if not message_photo_path:
        return None

    # КЛЮЧЕВО: video... возвращаем как есть, без повторной загрузки,
    # без проверки getById и без преобразования в document.
    if isinstance(message_photo_path, str) and message_photo_path.startswith(("photo", "doc", "video")):
        uploaded_message_photo = message_photo_path
        uploaded_message_photo_at = time.time()
        return uploaded_message_photo

    photo_path = Path(message_photo_path)
    if not photo_path.is_absolute():
        photo_path = BASE_DIR / photo_path

    if not photo_path.exists():
        logger.warning("Файл основного фото не найден: %s", photo_path)
        return None

    try:
        photo = media_vk_upload.photo_messages(
            str(photo_path),
            peer_id=int(admin_chat or 0) or None,
        )[0]
        uploaded_message_photo = f"photo{photo['owner_id']}_{photo['id']}"
        uploaded_message_photo_at = time.time()
        return uploaded_message_photo
    except Exception as exc:
        logger.error("Не удалось загрузить основное фото: %s", exc)
        return None



def _split_message(text: str, chunk_size: int = 3500) -> list[str]:
    """Безопасно разбивает длинный текст для VK сообщений.

    VK имеет ограничение на длину сообщения, поэтому ЛС режем на части.
    Функция всегда возвращает хотя бы один элемент.
    """
    value = str(text or " ")
    if len(value) <= chunk_size:
        return [value]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in value.splitlines(keepends=True):
        # Если одна строка сама длиннее лимита — режем её кусками.
        while len(line) > chunk_size:
            if current:
                chunks.append("".join(current).rstrip("\n"))
                current = []
                current_len = 0

            chunks.append(line[:chunk_size])
            line = line[chunk_size:]

        if current and current_len + len(line) > chunk_size:
            chunks.append("".join(current).rstrip("\n"))
            current = []
            current_len = 0

        current.append(line)
        current_len += len(line)

    if current:
        chunks.append("".join(current).rstrip("\n"))

    return [chunk if chunk else " " for chunk in chunks] or [" "]



def safe_send_pm(
    user_id: int,
    text: str,
    keyboard: str | None = None,
    *,
    attachment: str | None = None,
    rate_limit: bool = True,
    api_client: Any | None = None,
    bulk_rate_limit: bool = False,
    mark_activated_after_send: bool = True,
) -> Any:
    chunks = _split_message(text or " ")
    result = None
    use_primary_client = api_client is None and threading.get_ident() == _main_vk_thread_ident
    client = vk if use_primary_client else (api_client or get_bulk_vk())
    for i, chunk in enumerate(chunks):
        try:
            if bulk_rate_limit:
                wait_bulk_send_slot()
            elif rate_limit and use_primary_client:
                wait_service_send_cooldown()
            params = {"user_id": user_id, "message": chunk, "random_id": generate_random_id()}
            if keyboard and i == len(chunks) - 1:
                params["keyboard"] = keyboard
            if attachment and i == len(chunks) - 1:
                params["attachment"] = attachment
            result = client.messages.send(**params)
        except Exception as exc:
            error_text = str(exc)
            if "[901]" in error_text or "Can't send messages for users without permission" in error_text:
                logger.info(f"Не удалось отправить ЛС {user_id}: пользователь не разрешил сообщения сообщества.")
                return None
            logger.error(f"Ошибка отправки ЛС {user_id}: {exc}")
            return None
    # Массовая .инфозов уже работает по сохранённому списку активированных
    # пользователей. Не переписываем users_db.json после каждого получателя.
    if result is not None and mark_activated_after_send:
        mark_pm_activated(user_id)
    return result

def answer_callback_event(
    event_object: dict[str, Any],
    text: str = "Открываю...",
    *,
    link: str | None = None,
) -> None:
    try:
        event_data = {"type": "open_link", "link": link} if link else {"type": "show_snackbar", "text": text}
        vk.messages.sendMessageEventAnswer(
            event_id=event_object["event_id"],
            user_id=event_object["user_id"],
            peer_id=event_object["peer_id"],
            event_data=json.dumps(event_data, ensure_ascii=False),
        )
    except Exception as exc:
        logger.warning(f"Не удалось отправить callback-ответ: {exc}")

        return None


def render_income_stats() -> str:
    incomes = load_json_file(INCOMES_PATH, [])
    now = now_msk()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    def parse_created_at(item: dict[str, Any]) -> datetime | None:
        try:
            return datetime.strptime(item["created_at"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    day_income = sum(int(i.get("amount", 0) or 0) for i in incomes if (parse_created_at(i) and parse_created_at(i) >= day_start))
    week_income = sum(int(i.get("amount", 0) or 0) for i in incomes if (parse_created_at(i) and parse_created_at(i) >= week_ago))
    month_income = sum(int(i.get("amount", 0) or 0) for i in incomes if (parse_created_at(i) and parse_created_at(i) >= month_ago))
    total_income = sum(int(i.get("amount", 0) or 0) for i in incomes)
    return (
        "📊 Статистика доходов разработчика\n\n"
        f"📅 За сегодня: {day_income} ₽\n"
        f"📆 За неделю: {week_income} ₽\n"
        f"🗓 За месяц: {month_income} ₽\n"
        f"💎 За всё время: {total_income} ₽"
    )



BUTTON_STATS_KEEP_DAYS = 35
BUTTON_STATS_MENU_PAGE_SIZE = 8
BUTTON_CLICK_LOG_PAGE_SIZE = 20

# Считаем только кнопки, которые находятся непосредственно в рассылке.
BROADCAST_BUTTON_STATS_SOURCES = {
    "Основное сообщение",
    "Дополнительное сообщение",
    "Блок вакансий",
}


def _is_broadcast_button_key(key: str) -> bool:
    value = str(key or "").strip()
    return (
        value == "main_message_button"
        or value == "jobs_broadcast_button"
        or value.startswith("additional_ad:")
    )


def _is_broadcast_click_payload(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    key = str(payload.get("_stats_key") or "").strip()
    source = " ".join(str(payload.get("_stats_source") or "").split()).strip()
    return _is_broadcast_button_key(key) or source in BROADCAST_BUTTON_STATS_SOURCES


def _is_broadcast_click_event(event: dict[str, Any]) -> bool:
    if not isinstance(event, dict):
        return False
    key = str(event.get("key") or "").strip()
    source = " ".join(str(event.get("source") or "").split()).strip()
    return _is_broadcast_button_key(key) or source in BROADCAST_BUTTON_STATS_SOURCES


def _additional_button_stats_key(ad_data: dict[str, Any]) -> str:
    ad_identity = (
        str(ad_data.get("ad_key") or "").strip()
        or str(ad_data.get("order_id") or "").strip()
        or str(ad_data.get("user_id") or "").strip()
        or hashlib.sha1(
            (
                str(ad_data.get("text") or "")
                + "|"
                + str(ad_data.get("button_title") or "")
                + "|"
                + str(ad_data.get("button_link") or "")
            ).encode("utf-8", errors="ignore")
        ).hexdigest()[:12]
    )
    return f"additional_ad:{ad_identity}"


def _load_button_stats() -> dict[str, Any]:
    with _button_stats_lock:
        data = read_json(
            BUTTON_STATS_PATH,
            {"version": 1, "events": [], "links": {}},
        )
        if not isinstance(data, dict):
            data = {"version": 1, "events": [], "links": {}}
        if not isinstance(data.get("events"), list):
            data["events"] = []
        if not isinstance(data.get("links"), dict):
            data["links"] = {}
        data["version"] = 1
        return data


def _save_button_stats(data: dict[str, Any]) -> None:
    with _button_stats_lock:
        write_json_atomic(BUTTON_STATS_PATH, data)


def _prune_button_stats_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = time.time() - BUTTON_STATS_KEEP_DAYS * 24 * 60 * 60
    result: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        try:
            ts = float(event.get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0
        if ts >= cutoff:
            result.append(event)
    return result


def _button_stats_key_from_payload(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("_stats_key") or "").strip()
    if explicit:
        return explicit[:180]

    command = str(payload.get("command") or "unknown").strip().lower() or "unknown"

    # Для разных вакансий/заявок/заказов считаем кнопки отдельно.
    for field in ("job_id", "application_id", "order_id", "item_key", "company_id"):
        value = payload.get(field)
        if value not in {None, "", 0, "0"}:
            return f"{command}:{field}:{value}"[:180]

    return command[:180]


def _button_stats_description(payload: dict[str, Any]) -> tuple[str, str]:
    explicit_label = " ".join(str(payload.get("_stats_label") or "").split()).strip()
    explicit_source = " ".join(str(payload.get("_stats_source") or "").split()).strip()

    command = str(payload.get("command") or "").strip().lower()

    if explicit_label:
        return explicit_label[:100], (explicit_source or "Сообщения бота")[:100]

    labels = {
        "pm_jobs_deliver": ("Посмотреть вакансии", "Блок вакансий"),
        "pm_job_open": ("Открыть вакансию", "Каталог вакансий"),
        "pm_job_deliver": ("Подробнее о вакансии", "Рассылка вакансии"),
        "pm_job_interest": ("Интересно, вызвать Менеджера", "Карточка вакансии"),
        "pm_job_list": ("Посмотреть ещё вакансии / навигация", "Каталог вакансий"),
        "pm_access_check": ("Проверить вступление", "Личные сообщения"),
        "application_approve": ("Подтвердить заявку", "Админ / заявки"),
        "application_reject": ("Отклонить заявку", "Админ / заявки"),
        "stats_panel_latest": ("Последняя", "Панель статистики"),
        "stats_panel_day": ("Сегодня", "Панель статистики"),
        "stats_panel_week": ("7 дней", "Панель статистики"),
        "stats_panel_month": ("30 дней", "Панель статистики"),
        "stats_panel_chats": ("По чатам", "Панель статистики"),
    }

    if command.startswith("stats_panel_refresh_"):
        return "Обновить статистику", "Панель статистики"

    if command in labels:
        label, source = labels[command]
    else:
        label = command or "Неизвестная кнопка"
        source = explicit_source or "Другие кнопки"

    # Для вакансий подставляем название, если оно доступно.
    try:
        job_id = int(payload.get("job_id") or 0)
    except (TypeError, ValueError):
        job_id = 0
    if job_id and command in {"pm_job_open", "pm_job_deliver"}:
        try:
            job = get_pm_job(job_id)
        except Exception:
            job = None
        if isinstance(job, dict):
            title = " ".join(str(job.get("title") or "").split()).strip()
            if title:
                label = f"{label}: {title}"

    return label[:100], source[:100]


def register_tracked_link(
    title: str,
    link: str,
    *,
    stats_key: str,
    stats_source: str,
) -> str:
    """Регистрирует URL и возвращает короткий ID для callback-кнопки."""
    clean_link = str(link or "").strip()
    clean_title = " ".join(str(title or "").split()).strip() or "Открыть"
    clean_key = str(stats_key or "").strip() or "openlink"
    clean_source = " ".join(str(stats_source or "").split()).strip() or "Сообщения бота"

    link_id = hashlib.sha1(
        f"{clean_key}|{clean_link}".encode("utf-8", errors="ignore")
    ).hexdigest()[:16]

    with _button_stats_lock:
        data = _load_button_stats()
        links = data.setdefault("links", {})
        links[link_id] = {
            "url": clean_link,
            "label": clean_title[:100],
            "stats_key": clean_key[:180],
            "source": clean_source[:100],
            "updated_at": now_msk_str(),
        }
        _save_button_stats(data)

    return link_id


def build_tracked_openlink_keyboard(
    title: str,
    link: str,
    *,
    stats_key: str,
    stats_source: str,
    color: Any = VkKeyboardColor.PRIMARY,
) -> str:
    """Openlink через callback, чтобы VK прислал событие и клик можно было посчитать."""
    clean_link = str(link or "").strip()
    if "://" not in clean_link:
        clean_link = f"https://{clean_link}"

    link_id = register_tracked_link(
        title,
        clean_link,
        stats_key=stats_key,
        stats_source=stats_source,
    )

    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        str(title or "Открыть")[:40],
        color=color,
        payload={
            "command": "tracked_openlink",
            "link_id": link_id,
            "_stats_key": str(stats_key)[:180],
            "_stats_label": str(title or "Открыть")[:100],
            "_stats_source": str(stats_source)[:100],
        },
    )
    return keyboard.get_keyboard()


def record_button_click(peer_id: int, user_id: int, payload: dict[str, Any]) -> None:
    """Сохраняет нажатие только по кнопкам, находящимся в рассылке."""
    if not isinstance(payload, dict):
        return

    command = str(payload.get("command") or "").strip()
    if not command:
        return

    # Каталог, админка, заявки, навигация и другие внутренние кнопки НЕ считаются.
    if not _is_broadcast_click_payload(payload):
        return

    key = _button_stats_key_from_payload(payload)
    label, source = _button_stats_description(payload)

    event = {
        "ts": time.time(),
        "time": now_msk_str(),
        "key": key,
        "label": label,
        "source": source,
        "command": command[:100],
        "peer_id": int(peer_id or 0),
        "user_id": int(user_id or 0),
    }

    with _button_stats_lock:
        data = _load_button_stats()
        events = _prune_button_stats_events(data.get("events") or [])
        events.append(event)
        data["events"] = events
        _save_button_stats(data)


def handle_tracked_openlink_action(
    chat_id: int,
    user_id: int,
    payload: dict[str, Any],
) -> bool:
    if str(payload.get("command") or "").strip().lower() != "tracked_openlink":
        return False

    event_object = payload.get("_event_object")
    link_id = str(payload.get("link_id") or "").strip()

    data = _load_button_stats()
    item = (data.get("links") or {}).get(link_id)
    if not isinstance(item, dict):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Ссылка больше недоступна.")
        return True

    url = str(item.get("url") or "").strip()
    if not url:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Ссылка больше недоступна.")
        return True

    if isinstance(event_object, dict):
        answer_callback_event(event_object, link=url)
    return True


def _button_stats_rows(window_seconds: int) -> list[dict[str, Any]]:
    cutoff = time.time() - int(window_seconds)
    data = _load_button_stats()
    grouped: dict[str, dict[str, Any]] = {}

    for event in data.get("events") or []:
        if not isinstance(event, dict):
            continue
        try:
            ts = float(event.get("ts") or 0)
        except (TypeError, ValueError):
            continue
        if ts < cutoff:
            continue

        key = str(event.get("key") or event.get("command") or "unknown")
        row = grouped.setdefault(
            key,
            {
                "key": key,
                "label": str(event.get("label") or key),
                "source": str(event.get("source") or "Другие кнопки"),
                "clicks": 0,
                "users": set(),
            },
        )
        row["clicks"] += 1
        try:
            uid = int(event.get("user_id") or 0)
        except (TypeError, ValueError):
            uid = 0
        if uid:
            row["users"].add(uid)

    rows = list(grouped.values())
    rows.sort(key=lambda item: (-int(item["clicks"]), str(item["source"]), str(item["label"])))
    return rows


def render_button_stats() -> str:
    windows = [
        ("24 часа", 24 * 60 * 60),
        ("7 дней", 7 * 24 * 60 * 60),
        ("30 дней", 30 * 24 * 60 * 60),
    ]

    sections = ["📊 СТАТИСТИКА КНОПОК"]

    for title, seconds in windows:
        rows = _button_stats_rows(seconds)
        total_clicks = sum(int(row["clicks"]) for row in rows)

        sections.append("")
        sections.append(f"━━ {title} ━━")
        sections.append(f"Всего нажатий: {total_clicks}")

        if not rows:
            sections.append("Нажатий пока нет.")
            continue

        current_source = None
        for row in rows:
            source = str(row["source"] or "Другие кнопки")
            if source != current_source:
                sections.append("")
                sections.append(f"• {source}")
                current_source = source

            users_count = len(row["users"]) if isinstance(row.get("users"), set) else 0
            sections.append(
                f"  {row['label']} — {row['clicks']} "
                f"(людей: {users_count})"
            )

    sections.append("")
    sections.append("Статистика считается с момента установки этой версии бота.")
    return "\n".join(sections)




def _get_current_broadcast_buttons() -> list[dict[str, str]]:
    """Только кнопки, которые прямо сейчас входят в рассылку."""
    result: list[dict[str, str]] = []

    # Основное сообщение.
    if str(message_text or "").strip() and str(main_button_title or "").strip():
        result.append({
            "key": "main_message_button",
            "label": str(main_button_title).strip(),
            "source": "Основное сообщение",
        })

    # Компактный блок вакансий.
    jobs_block = get_jobs_broadcast_message()
    if jobs_block.get("enabled") and str(jobs_block.get("text") or "").strip():
        result.append({
            "key": "jobs_broadcast_button",
            "label": "Посмотреть вакансии",
            "source": "Блок вакансий",
        })

    current_ads = order_service.load_ads()
    additional_items: list[dict[str, str]] = []

    if isinstance(current_ads, dict):
        for ad_key, ad in current_ads.items():
            if not isinstance(ad, dict):
                continue
            if ad_key in {"main_text_sale", "active_ad", "users"}:
                continue
            if is_user_mirror_ad_key(ad_key, ad):
                continue
            if ad.get("status") not in {"approved", "active"}:
                continue
            if ad.get("broadcast_paused"):
                continue
            if jobs_broadcast_compact_enabled() and is_internal_job_broadcast_ad(ad):
                continue

            expires_at = ad.get("expires_at")
            if expires_at:
                try:
                    if datetime.strptime(
                        expires_at,
                        "%Y-%m-%d %H:%M:%S",
                    ).replace(tzinfo=MSK) < now_msk():
                        continue
                except ValueError:
                    pass

            title = " ".join(str(ad.get("button_title") or "").split()).strip()
            link = str(ad.get("button_link") or "").strip()
            if not title or not link:
                continue

            ad_for_key = dict(ad)
            ad_for_key.setdefault("ad_key", str(ad_key))
            additional_items.append({
                "key": _additional_button_stats_key(ad_for_key),
                "label": title,
                "source": "Дополнительное сообщение",
            })

        main_text_sale = current_ads.get("main_text_sale", {})
        if isinstance(main_text_sale, dict):
            is_active = (
                main_text_sale.get("status") == "active"
                and not main_text_sale.get("broadcast_paused")
            )
            expires_at = main_text_sale.get("expires_at")
            if is_active and expires_at:
                try:
                    is_active = (
                        datetime.strptime(
                            expires_at,
                            "%Y-%m-%d %H:%M:%S",
                        ).replace(tzinfo=MSK)
                        > now_msk()
                    )
                except ValueError:
                    pass

            title = " ".join(str(main_text_sale.get("button_title") or "").split()).strip()
            link = str(main_text_sale.get("button_link") or "").strip()

            if is_active and title and link:
                ad_for_key = dict(main_text_sale)
                ad_for_key.setdefault("ad_key", "main_text_sale")
                additional_items.append({
                    "key": _additional_button_stats_key(ad_for_key),
                    "label": title,
                    "source": "Дополнительное сообщение",
                })

    # Основная кнопка и вакансии сверху, остальные стабильно по названию.
    seen: set[str] = {item["key"] for item in result}
    for item in sorted(additional_items, key=lambda x: (x["label"].lower(), x["key"])):
        if item["key"] in seen:
            continue
        seen.add(item["key"])
        result.append(item)

    return result


def _find_broadcast_button_meta(stats_key: str) -> dict[str, str] | None:
    key = str(stats_key or "").strip()

    for item in _get_current_broadcast_buttons():
        if item["key"] == key:
            return item

    # Если сообщение уже снято с рассылки, но по нему были клики — имя берём из истории.
    data = _load_button_stats()
    for event in reversed(data.get("events") or []):
        if not _is_broadcast_click_event(event):
            continue
        if str(event.get("key") or "") == key:
            return {
                "key": key,
                "label": str(event.get("label") or key),
                "source": str(event.get("source") or "Рассылка"),
            }

    return None


def build_button_stats_menu_keyboard(page: int = 0) -> tuple[str | None, int, int]:
    items = _get_current_broadcast_buttons()
    if not items:
        return None, 0, 1

    total_pages = max(
        1,
        (len(items) + BUTTON_STATS_MENU_PAGE_SIZE - 1) // BUTTON_STATS_MENU_PAGE_SIZE,
    )
    safe_page = max(0, min(int(page or 0), total_pages - 1))
    start = safe_page * BUTTON_STATS_MENU_PAGE_SIZE
    batch = items[start:start + BUTTON_STATS_MENU_PAGE_SIZE]

    keyboard = VkKeyboard(inline=True)

    for index, item in enumerate(batch):
        button_text = f"Кнопка: {item['label']}"
        if len(button_text) > 40:
            button_text = button_text[:37].rstrip() + "..."

        keyboard.add_callback_button(
            button_text,
            color=VkKeyboardColor.PRIMARY,
            payload={
                "command": "button_stats_open",
                "target_key": item["key"],
                "menu_page": safe_page,
            },
        )
        if index < len(batch) - 1:
            keyboard.add_line()

    if total_pages > 1:
        keyboard.add_line()
        if safe_page > 0:
            keyboard.add_callback_button(
                "← Назад",
                color=VkKeyboardColor.SECONDARY,
                payload={
                    "command": "button_stats_menu",
                    "menu_page": safe_page - 1,
                },
            )
        if safe_page < total_pages - 1:
            keyboard.add_callback_button(
                "Далее →",
                color=VkKeyboardColor.SECONDARY,
                payload={
                    "command": "button_stats_menu",
                    "menu_page": safe_page + 1,
                },
            )

    return keyboard.get_keyboard(), safe_page, total_pages


def render_button_stats_menu(page: int = 0) -> tuple[str, str | None]:
    keyboard, safe_page, total_pages = build_button_stats_menu_keyboard(page)
    items = _get_current_broadcast_buttons()

    if not items:
        return (
            "📊 СТАТИСТИКА КНОПОК РАССЫЛКИ\n\n"
            "Сейчас в рассылке нет кнопок.",
            None,
        )

    text_value = (
        "📊 СТАТИСТИКА КНОПОК РАССЫЛКИ\n\n"
        "Выберите кнопку ниже.\n"
        "В статистику попадают ТОЛЬКО кнопки из рассылки.\n\n"
        f"Кнопок в рассылке: {len(items)}"
    )
    if total_pages > 1:
        text_value += f"\nСтраница: {safe_page + 1}/{total_pages}"

    return text_value, keyboard


def _broadcast_button_events(stats_key: str) -> list[dict[str, Any]]:
    key = str(stats_key or "").strip()
    data = _load_button_stats()

    result = [
        event
        for event in data.get("events") or []
        if isinstance(event, dict)
        and _is_broadcast_click_event(event)
        and str(event.get("key") or "") == key
    ]
    result.sort(key=lambda event: float(event.get("ts") or 0))
    return result


def _count_button_events_since(events: list[dict[str, Any]], seconds: int) -> int:
    cutoff = time.time() - int(seconds)
    return sum(
        1
        for event in events
        if float(event.get("ts") or 0) >= cutoff
    )


def _peak_click_hour(
    events: list[dict[str, Any]],
    seconds: int = 30 * 24 * 60 * 60,
) -> tuple[int | None, int]:
    cutoff = time.time() - int(seconds)
    buckets: dict[int, int] = {}

    for event in events:
        try:
            ts = float(event.get("ts") or 0)
        except (TypeError, ValueError):
            continue
        if ts < cutoff:
            continue

        hour = datetime.fromtimestamp(ts, tz=MSK).hour
        buckets[hour] = buckets.get(hour, 0) + 1

    if not buckets:
        return None, 0

    return max(buckets.items(), key=lambda item: (item[1], -item[0]))


def render_broadcast_button_stats_detail(
    stats_key: str,
    menu_page: int = 0,
) -> tuple[str, str]:
    meta = _find_broadcast_button_meta(stats_key) or {
        "key": stats_key,
        "label": stats_key,
        "source": "Рассылка",
    }

    events = _broadcast_button_events(stats_key)

    day_count = _count_button_events_since(events, 24 * 60 * 60)
    week_count = _count_button_events_since(events, 7 * 24 * 60 * 60)
    month_count = _count_button_events_since(events, 30 * 24 * 60 * 60)

    peak_hour, peak_count = _peak_click_hour(events)
    if peak_hour is None:
        peak_text = "пока нет данных"
    else:
        peak_text = (
            f"{peak_hour:02d}:00–{peak_hour:02d}:59 МСК "
            f"— {peak_count} нажатий"
        )

    last_click = "нет"
    if events:
        last_click = str(events[-1].get("time") or "нет") + " МСК"

    text_value = (
        "📊 СТАТИСТИКА КНОПКИ\n\n"
        f"Кнопка: {meta['label']}\n"
        f"Сообщение: {meta['source']}\n\n"
        f"За 1 день: {day_count}\n"
        f"За 7 дней: {week_count}\n"
        f"За 30 дней: {month_count}\n"
        f"Всего сохранено: {len(events)}\n\n"
        "🔥 Лидер времени по нажатиям за 30 дней:\n"
        f"{peak_text}\n\n"
        f"Последнее нажатие: {last_click}"
    )

    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "← К списку кнопок",
        color=VkKeyboardColor.SECONDARY,
        payload={
            "command": "button_stats_menu",
            "menu_page": int(menu_page or 0),
        },
    )

    return text_value, keyboard.get_keyboard()


def show_button_stats_menu(
    chat_id: int,
    user_id: int,
    *,
    page: int = 0,
    event_object: dict[str, Any] | None = None,
) -> None:
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Недостаточно прав.")
        return

    text_value, keyboard = render_button_stats_menu(page)

    if isinstance(event_object, dict):
        answer_callback_event(event_object, text="Открываю.")
        if keyboard and _edit_stats_panel_from_event(
            chat_id,
            text_value,
            keyboard,
            event_object,
        ):
            return

    send_message(
        chat_id,
        text_value,
        keyboard=keyboard,
        rate_limit=False,
    )


def handle_button_stats_menu_action(
    chat_id: int,
    user_id: int,
    payload: dict[str, Any],
) -> bool:
    command = str(payload.get("command") or "").strip().lower()
    if command not in {"button_stats_menu", "button_stats_open"}:
        return False

    event_object = payload.get("_event_object")

    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Недостаточно прав.")
        return True

    if command == "button_stats_menu":
        try:
            page = max(0, int(payload.get("menu_page") or 0))
        except (TypeError, ValueError):
            page = 0

        show_button_stats_menu(
            chat_id,
            user_id,
            page=page,
            event_object=event_object,
        )
        return True

    stats_key = str(payload.get("target_key") or "").strip()
    try:
        menu_page = max(0, int(payload.get("menu_page") or 0))
    except (TypeError, ValueError):
        menu_page = 0

    if not stats_key:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Кнопка не найдена.")
        return True

    text_value, keyboard = render_broadcast_button_stats_detail(
        stats_key,
        menu_page=menu_page,
    )

    if isinstance(event_object, dict):
        answer_callback_event(event_object, text="Статистика кнопки.")
        if _edit_stats_panel_from_event(
            chat_id,
            text_value,
            keyboard,
            event_object,
        ):
            return True

    send_message(
        chat_id,
        text_value,
        keyboard=keyboard,
        rate_limit=False,
    )
    return True


def _parse_button_click_log_page(command_text: str) -> int:
    parts = str(command_text or "").strip().split()
    if len(parts) >= 2:
        try:
            return max(1, int(parts[1]))
        except (TypeError, ValueError):
            pass
    return 1


def render_button_click_log(page: int = 1) -> str:
    data = _load_button_stats()

    events = [
        event
        for event in data.get("events") or []
        if isinstance(event, dict) and _is_broadcast_click_event(event)
    ]
    events.sort(
        key=lambda event: float(event.get("ts") or 0),
        reverse=True,
    )

    total = len(events)
    if not total:
        return (
            "🕒 СТАТИСТИКА НАЖАТИЙ\n\n"
            "Нажатий по кнопкам рассылки пока нет."
        )

    total_pages = max(
        1,
        (total + BUTTON_CLICK_LOG_PAGE_SIZE - 1) // BUTTON_CLICK_LOG_PAGE_SIZE,
    )
    safe_page = max(1, min(int(page or 1), total_pages))
    start = (safe_page - 1) * BUTTON_CLICK_LOG_PAGE_SIZE
    batch = events[start:start + BUTTON_CLICK_LOG_PAGE_SIZE]

    lines = [
        "🕒 СТАТИСТИКА НАЖАТИЙ",
        "",
        "Каждое нажатие по кнопкам рассылки сохраняется с датой и временем.",
        f"Всего записей: {total}",
        f"Страница: {safe_page}/{total_pages}",
        "",
    ]

    for index, event in enumerate(batch, start=start + 1):
        stored_time = str(event.get("time") or "").strip()
        if not stored_time:
            try:
                stored_time = datetime.fromtimestamp(
                    float(event.get("ts") or 0),
                    tz=MSK,
                ).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                stored_time = "время неизвестно"

        label = " ".join(
            str(
                event.get("label")
                or event.get("key")
                or "Кнопка"
            ).split()
        ).strip()

        try:
            click_user_id = int(event.get("user_id") or 0)
        except (TypeError, ValueError):
            click_user_id = 0

        lines.append(
            f"{index}. {stored_time} МСК\n"
            f"Кнопка: {label}\n"
            f"Пользователь: id{click_user_id if click_user_id else 'неизвестен'}"
        )

    if safe_page < total_pages:
        lines.append("")
        lines.append(f"Следующая: .статистиканажатий {safe_page + 1}")

    return "\n\n".join(lines)



def build_buy_ad_keyboard() -> str:
    link = main_button_link
    if "://" not in link:
        link = f"https://{link}"
    job_id = extract_pm_job_id_from_link(link)
    if job_id:
        return build_pm_job_delivery_keyboard(
            job_id,
            main_button_title,
            stats_source="Основное сообщение",
            stats_key="main_message_button",
        )
    return build_tracked_openlink_keyboard(
        main_button_title,
        link,
        stats_key="main_message_button",
        stats_source="Основное сообщение",
        color=VkKeyboardColor.PRIMARY,
    )


def build_main_button_edit_keyboard() -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "✏️ Редактировать текст",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_main_button_title"},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "🔗 Редактировать ссылку",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_main_button_link"},
    )
    return keyboard.get_keyboard()


def handle_main_button_edit_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    command = str(payload.get("command") or "").strip().lower()
    if command not in {"edit_main_button_title", "edit_main_button_link"}:
        return False
    if not has_permission(user_id, "dev"):
        send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
        return True
    if chat_id != admin_chat:
        send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
        return True
    mode = "title" if command == "edit_main_button_title" else "link"
    USER_STATES.setdefault(str(user_id), {})["main_button_edit"] = {"mode": mode, "chat_id": chat_id}
    if mode == "title":
        send_message(chat_id, f"✏️ Отправьте новое название кнопки.\nТекущее: {main_button_title}")
    else:
        send_message(chat_id, f"🔗 Отправьте новую ссылку кнопки, начиная с http:// или https://\nТекущая: {main_button_link}")
    return True


def handle_main_button_edit_input(chat_id: int, user_id: int, text: str) -> bool:
    global main_button_title, main_button_link
    state = USER_STATES.get(str(user_id)) or {}
    pending = state.get("main_button_edit")
    if not isinstance(pending, dict) or pending.get("chat_id") != chat_id:
        return False
    if chat_id != admin_chat or not has_permission(user_id, "dev"):
        state.pop("main_button_edit", None)
        send_message(chat_id, "❌ Права разработчика больше не подтверждены. Редактирование отменено.")
        return True
    mode = pending.get("mode")
    value = text.strip()
    if mode == "title":
        if not value:
            send_message(chat_id, "❌ Название кнопки не может быть пустым.")
            return True
        main_button_title = value[:40]
        state.pop("main_button_edit", None)
        save_runtime_data()
        send_message(chat_id, f"✅ Название основной кнопки изменено на: {main_button_title}", keyboard=build_buy_ad_keyboard())
        return True
    if mode == "link":
        if not re.match(r"^https?://", value, flags=re.IGNORECASE):
            send_message(chat_id, "❌ Ссылка должна начинаться с http:// или https://")
            return True
        main_button_link = value
        state.pop("main_button_edit", None)
        save_runtime_data()
        send_message(chat_id, f"✅ Ссылка основной кнопки изменена на: {main_button_link}", keyboard=build_buy_ad_keyboard())
        return True
    state.pop("main_button_edit", None)
    return False


def build_ad_openlink_keyboard(ad_data: dict[str, Any]) -> str | None:
    title = str(ad_data.get("button_title") or "").strip()
    link = str(ad_data.get("button_link") or "").strip()
    if not title or not link:
        return None
    if "://" not in link:
        link = f"https://{link}"

    stats_key = _additional_button_stats_key(ad_data)

    job_id = extract_pm_job_id_from_link(link)
    if job_id:
        return build_pm_job_delivery_keyboard(
            job_id,
            title,
            stats_source="Дополнительное сообщение",
            stats_key=stats_key,
        )

    return build_tracked_openlink_keyboard(
        title,
        link,
        stats_key=stats_key,
        stats_source="Дополнительное сообщение",
        color=VkKeyboardColor.PRIMARY,
    )


def extract_attachment_reference(message: dict[str, Any]) -> str | None:
    attachments = message.get("attachments") or []
    if not attachments:
        return None
    raw = attachments[0]
    kind = raw.get("type")
    if kind not in {"photo", "video", "doc"}:
        return None
    item = raw[kind]
    reference = f"{kind}{item['owner_id']}_{item['id']}"
    if item.get("access_key"):
        reference = f"{reference}_{item['access_key']}"
    return reference


def _normalize_attachment_reference(reference: str | None) -> str:
    value = str(reference or "").strip()
    if not value:
        return ""
    parts = value.split("_")
    if len(parts) >= 3 and parts[0].startswith(("photo", "video", "doc")):
        return "_".join(parts[:2])
    return value


def primary_photo_reference_from_order(order: dict[str, Any] | None) -> str | None:
    if not order:
        return None
    photo_value = str(order.get("photo") or "").strip()
    if not photo_value:
        return None
    parts = [part.strip() for part in photo_value.split(",") if part.strip()]
    if not parts:
        return None
    primary_photo = parts[0]
    check_reference = check_attachment_reference_from_order(order)
    if check_reference and _normalize_attachment_reference(primary_photo) == _normalize_attachment_reference(check_reference):
        return None
    return primary_photo


def check_attachment_reference_from_order(order: dict[str, Any] | None) -> str | None:
    if not order:
        return None
    attachment = order.get("attachment")
    if isinstance(attachment, str):
        clean = attachment.strip()
        return clean or None
    if isinstance(attachment, list):
        for item in attachment:
            if isinstance(item, str) and item.strip():
                return item.strip()
    if isinstance(attachment, dict):
        if attachment.get("vk_attachment"):
            return str(attachment["vk_attachment"])

        payload = attachment.get("payload")
        if isinstance(payload, dict):
            kind = payload.get("type")
            nested = payload.get(kind) if kind in {"photo", "doc", "video"} else None
            if isinstance(nested, dict) and nested.get("owner_id") and nested.get("id"):
                reference = f"{kind}{nested['owner_id']}_{nested['id']}"
                if nested.get("access_key"):
                    reference = f"{reference}_{nested['access_key']}"
                return reference

        kind = attachment.get("type")
        if kind in {"photo", "doc", "video"}:
            nested = attachment.get(kind)
            if isinstance(nested, dict) and nested.get("owner_id") and nested.get("id"):
                reference = f"{kind}{nested['owner_id']}_{nested['id']}"
                if nested.get("access_key"):
                    reference = f"{reference}_{nested['access_key']}"
                return reference
            if attachment.get("owner_id") and attachment.get("id"):
                reference = f"{kind}{attachment['owner_id']}_{attachment['id']}"
                if attachment.get("access_key"):
                    reference = f"{reference}_{attachment['access_key']}"
                return reference
    return None


def check_file_link_from_order(order: dict[str, Any] | None) -> str | None:
    if not order:
        return None
    attachment = order.get("attachment")
    if not isinstance(attachment, dict):
        return None
    for candidate in (attachment, attachment.get("payload")):
        if not isinstance(candidate, dict):
            continue
        if candidate.get("url"):
            return str(candidate["url"])
        doc = candidate.get("doc")
        if isinstance(doc, dict) and doc.get("url"):
            return str(doc["url"])
    return None


def check_file_title_from_order(order: dict[str, Any] | None) -> str:
    attachment = (order or {}).get("attachment")
    if isinstance(attachment, dict):
        for candidate in (attachment, attachment.get("payload")):
            if not isinstance(candidate, dict):
                continue
            if candidate.get("title"):
                return str(candidate["title"])
            doc = candidate.get("doc")
            if isinstance(doc, dict) and doc.get("title"):
                return str(doc["title"])
    return "файл чека"


def extract_attachment_references(message: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for raw in message.get("attachments") or []:
        kind = raw.get("type")
        if kind not in {"photo", "video", "doc"}:
            continue
        item = raw[kind]
        reference = f"{kind}{item['owner_id']}_{item['id']}"
        if item.get("access_key"):
            reference = f"{reference}_{item['access_key']}"
        refs.append(reference)
    return refs


def prepare_pm_job_attachments(
    message: dict[str, Any],
    *,
    prefix: str,
) -> tuple[list[str], list[str | None]]:
    """Сохраняет резервную копию фото вакансии и делает собственную VK-копию."""
    raw_attachments = [
        raw for raw in (message.get("attachments") or [])
        if isinstance(raw, dict) and raw.get("type") in {"photo", "video", "doc"}
    ]
    if not raw_attachments:
        return [], []

    refs = extract_attachment_references({"attachments": raw_attachments})
    files = save_photo_files_from_attachments(
        BASE_DIR,
        raw_attachments,
        prefix=prefix,
        allowed_kinds={"photo", "video", "doc"},
        logger=logger,
    )

    durable_refs: list[str] = []
    for index, reference in enumerate(refs):
        ready = reference
        if reference.startswith("photo"):
            local_file = files[index] if index < len(files) else None
            uploaded = _upload_local_ad_photo(local_file)
            if uploaded:
                ready = uploaded
        durable_refs.append(ready)
    return durable_refs, files


def _recover_pm_job_photo_file(reference: str, job_id: int, index: int) -> str | None:
    """Пытается восстановить старую фотографию вакансии, пока VK ещё отдаёт её."""
    if not reference.startswith("photo"):
        return None
    # photos.getById принимает owner_id_photo_id[_access_key], без префикса photo.
    photo_id = reference[5:]
    try:
        items = media_vk.photos.getById(photos=photo_id, photo_sizes=1)
    except Exception as exc:
        logger.warning(
            "Не удалось восстановить старое фото вакансии #%s (%s): %s",
            job_id,
            reference,
            exc,
        )
        return None
    if not items:
        return None
    return save_photo_payload(
        BASE_DIR,
        items[0],
        prefix=f"pm_job_recovered_{job_id}",
        index=index,
        logger=logger,
    )


def resolve_pm_job_attachment(job: dict[str, Any]) -> str | None:
    """Возвращает рабочее вложение и автоматически мигрирует старые фото."""
    refs = split_attachment_refs(job.get("attachment"))
    if not refs:
        return None

    try:
        job_id = int(job.get("id", 0) or 0)
    except (TypeError, ValueError):
        job_id = 0

    files = normalize_photo_files(job.get("attachment_files"))
    resolved: list[str] = []
    changed = False

    for index, reference in enumerate(refs):
        if not reference.startswith("photo"):
            resolved.append(reference)
            continue

        local_file = files[index] if index < len(files) else None
        uploaded = _upload_local_ad_photo(local_file)

        if not uploaded:
            recovered = _recover_pm_job_photo_file(reference, job_id, index)
            if recovered:
                while len(files) <= index:
                    files.append(None)
                files[index] = recovered
                uploaded = _upload_local_ad_photo(recovered)
                changed = True

        ready = uploaded or reference
        if uploaded and uploaded != reference:
            changed = True
        resolved.append(ready)

    attachment = ",".join(resolved) if resolved else None
    if changed and job_id:
        update_pm_job(
            job_id,
            {
                "attachment": attachment or "",
                "attachment_files": files,
            },
        )
        job["attachment"] = attachment or ""
        job["attachment_files"] = files

    return attachment


def _resolve_local_media_path(relative_path: str | None) -> Path | None:
    if not relative_path:
        return None
    path = Path(relative_path)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path if path.exists() else None


def _upload_local_ad_photo(relative_path: str | None) -> str | None:
    with _media_upload_lock:
        return _upload_local_ad_photo_unlocked(relative_path)


def _upload_local_ad_photo_unlocked(relative_path: str | None) -> str | None:
    path = _resolve_local_media_path(relative_path)
    if path is None:
        return None

    try:
        stat = path.stat()
    except OSError:
        return None

    cache_key = f"{path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
    cached = uploaded_ad_photo_cache.get(cache_key)
    if cached and time.time() - cached[1] < UPLOADED_PHOTO_MAX_AGE:
        return cached[0]

    # Для group token peer_id важен: используем админ-чат как upload destination.
    upload_peer_id = int(admin_chat or 0)

    def _try_upload(candidate: Path) -> str | None:
        try:
            result = media_vk_upload.photo_messages(
                str(candidate),
                peer_id=upload_peer_id if upload_peer_id else None,
            )
            if not result:
                return None
            photo = result[0]
            return f"photo{photo['owner_id']}_{photo['id']}"
        except Exception as exc:
            logger.warning(
                "Не удалось загрузить локальное фото рекламы %s: %s",
                candidate,
                exc,
            )
            return None

    uploaded_ref = _try_upload(path)

    # Некоторые сохранённые jpg фактически оказываются WEBP/PNG/HEIC.
    # Если Pillow присутствует, нормализуем картинку в обычный RGB JPEG и повторяем.
    if not uploaded_ref:
        normalized_path = path.with_name(path.stem + "_vk.jpg")
        try:
            from PIL import Image  # type: ignore

            with Image.open(path) as image:
                if image.mode not in {"RGB", "L"}:
                    image = image.convert("RGB")
                elif image.mode == "L":
                    image = image.convert("RGB")
                image.save(normalized_path, format="JPEG", quality=94, optimize=True)

            uploaded_ref = _try_upload(normalized_path)
        except Exception as exc:
            logger.debug("Нормализация фото %s не выполнена: %s", path, exc)

    if not uploaded_ref:
        return None

    uploaded_ad_photo_cache[cache_key] = (uploaded_ref, time.time())
    return uploaded_ref


def _recover_photo_file_from_vk(reference: str, cache_key: str, index: int) -> str | None:
    failed_at = failed_ad_photo_recoveries.get(reference)
    if failed_at and time.time() - failed_at < 3600:
        return None
    try:
        items = media_vk.photos.getById(photos=reference, photo_sizes=1)
    except Exception as exc:
        failed_ad_photo_recoveries[reference] = time.time()
        logger.warning(f"Не удалось восстановить фото рекламы {reference}: {exc}")
        return None
    if not items:
        failed_ad_photo_recoveries[reference] = time.time()
        return None
    result = save_photo_payload(
        BASE_DIR,
        items[0],
        prefix=f"recovered_{cache_key}",
        index=index,
        logger=logger,
    )
    if result:
        failed_ad_photo_recoveries.pop(reference, None)
    else:
        failed_ad_photo_recoveries[reference] = time.time()
    return result


def resolve_ad_photo_attachment(ad_data: dict[str, Any], cache_key: str) -> tuple[str | None, bool]:
    refs = split_attachment_refs(ad_data.get("photo"))
    if not refs:
        return None, False

    photo_files = normalize_photo_files(ad_data.get("photo_files"))
    changed = False
    resolved_refs: list[str] = []

    for index, reference in enumerate(refs):
        if not reference.startswith("photo"):
            resolved_refs.append(reference)
            continue

        local_file = photo_files[index] if index < len(photo_files) else None
        uploaded_ref = _upload_local_ad_photo(local_file)
        if not uploaded_ref:
            recovered_file = _recover_photo_file_from_vk(reference, cache_key, index)
            if recovered_file:
                while len(photo_files) <= index:
                    photo_files.append(None)
                photo_files[index] = recovered_file
                changed = True
                uploaded_ref = _upload_local_ad_photo(recovered_file)

        resolved_refs.append(uploaded_ref or reference)

    if changed:
        ad_data["photo_files"] = photo_files
    return ",".join(resolved_refs) if resolved_refs else None, changed


def jobs_broadcast_compact_enabled() -> bool:
    block = get_jobs_broadcast_message()
    return bool(block.get("enabled") and str(block.get("text") or "").strip())


def is_internal_job_broadcast_ad(ad_data: dict[str, Any]) -> bool:
    """Вакансия из внутреннего каталога, которую заменяет единый блок меню."""
    if not isinstance(ad_data, dict):
        return False
    job_id = extract_pm_job_id_from_link(ad_data.get("button_link"))
    if not job_id:
        return False
    return get_pm_job(job_id, active_only=True) is not None


def get_active_random_orders() -> list[tuple[str, dict[str, Any]]]:
    current_ads = ads_data if isinstance(ads_data, dict) else order_service.load_ads()
    active_items: list[tuple[str, dict[str, Any]]] = []
    for user_key, ad_data in current_ads.items():
        if not isinstance(ad_data, dict):
            continue
        if user_key in {"main_text_sale", "active_ad", "users"}:
            continue
        if is_user_mirror_ad_key(user_key, ad_data):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue
        if ad_data.get("broadcast_paused"):
            continue
        # Когда включён компактный блок вакансий, внутренние вакансии с кнопкой
        # на job_N не рассылаются по одной. Вместо 10–15 сообщений чат получает
        # один блок «Посмотреть вакансии». Разовая .отпрзаказ при этом не меняется.
        if jobs_broadcast_compact_enabled() and is_internal_job_broadcast_ad(ad_data):
            continue
        expires_at = ad_data.get("expires_at")
        if expires_at:
            try:
                if datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK) < now_msk():
                    continue
            except ValueError:
                pass
        active_items.append((user_key, ad_data))
    random.shuffle(active_items)
    return active_items


def get_jobs_broadcast_message() -> dict[str, Any]:
    with _runtime_data_lock:
        raw = runtime_data.get("jobs_broadcast_message", {})
        return dict(raw) if isinstance(raw, dict) else {}


def build_jobs_broadcast_keyboard() -> str:
    """Кнопка из чатов: открывает ЛС и сразу просит каталог."""
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "Посмотреть вакансии",
        color=VkKeyboardColor.POSITIVE,
        payload={
            "command": "pm_jobs_deliver",
            "page": 0,
            "_stats_key": "jobs_broadcast_button",
            "_stats_label": "Посмотреть вакансии",
            "_stats_source": "Блок вакансий",
        },
    )
    return keyboard.get_keyboard()


def resolve_jobs_broadcast_attachment(block: dict[str, Any]) -> tuple[str | None, bool]:
    # Используем ту же надёжную схему восстановления локальных фото, что и у заказов.
    if not isinstance(block, dict):
        return None, False
    return resolve_ad_photo_attachment(block, "jobs_broadcast_message")


def save_jobs_broadcast_message_from_admin(
    text_value: str,
    message: dict[str, Any],
    user_id: int,
) -> dict[str, Any]:
    clean_text = str(text_value or "").strip()
    with _runtime_data_lock:
        current = runtime_data.get("jobs_broadcast_message", {})
        block = dict(current) if isinstance(current, dict) else {}

    media_attachments = [
        raw for raw in (message.get("attachments") or [])
        if isinstance(raw, dict) and raw.get("type") in {"photo", "video"}
    ]
    if media_attachments:
        refs: list[str] = []
        for raw in media_attachments:
            kind = raw.get("type")
            item = raw.get(kind) if isinstance(raw.get(kind), dict) else {}
            if kind not in {"photo", "video"} or not item:
                continue
            ref = f"{kind}{item.get('owner_id')}_{item.get('id')}"
            if item.get("access_key"):
                ref += f"_{item.get('access_key')}"
            refs.append(ref)
        files = save_photo_files_from_attachments(
            BASE_DIR,
            media_attachments,
            prefix="jobs_broadcast_message",
            allowed_kinds={"photo", "video"},
            logger=logger,
        )
        block["photo"] = ",".join(refs) if refs else None
        block["photo_files"] = files

    block.update({
        "enabled": True,
        "text": clean_text,
        "updated_at": now_msk_str(),
        "updated_by": int(user_id),
    })
    with _runtime_data_lock:
        runtime_data["jobs_broadcast_message"] = block
        save_runtime_data()
    return dict(block)


def delete_jobs_broadcast_message() -> None:
    with _runtime_data_lock:
        runtime_data["jobs_broadcast_message"] = {}
        save_runtime_data()


def build_jobs_broadcast_edit_keyboard(requester_id: int, preview_message_id: int | None = None) -> str:
    """Админское меню редактирования компактного блока вакансий."""
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "✏️ Редактировать текст",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_jobs_broadcast_text", "requester_id": int(requester_id)},
    )
    keyboard.add_callback_button(
        "🖼 Удалить фото",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": "remove_jobs_broadcast_photo", "requester_id": int(requester_id)},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📎 Добавить ещё фото",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": "add_jobs_broadcast_photo", "requester_id": int(requester_id)},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "❌ Выйти",
        color=VkKeyboardColor.SECONDARY,
        payload={
            "command": "exit_jobs_broadcast_edit",
            "requester_id": int(requester_id),
            "preview_message_id": int(preview_message_id or 0),
        },
    )
    return keyboard.get_keyboard()


def update_jobs_broadcast_text(text_value: str, user_id: int) -> bool:
    clean_text = str(text_value or "").strip()
    if not clean_text:
        return False
    with _runtime_data_lock:
        current = runtime_data.get("jobs_broadcast_message", {})
        if not isinstance(current, dict) or not current.get("enabled"):
            return False
        block = dict(current)
        block["text"] = clean_text
        block["updated_at"] = now_msk_str()
        block["updated_by"] = int(user_id)
        runtime_data["jobs_broadcast_message"] = block
        save_runtime_data()
    return True


def remove_jobs_broadcast_photo(user_id: int) -> bool:
    with _runtime_data_lock:
        current = runtime_data.get("jobs_broadcast_message", {})
        if not isinstance(current, dict) or not current.get("enabled"):
            return False
        block = dict(current)
        block["photo"] = None
        block["photo_files"] = []
        block["updated_at"] = now_msk_str()
        block["updated_by"] = int(user_id)
        runtime_data["jobs_broadcast_message"] = block
        save_runtime_data()
    return True


def add_jobs_broadcast_media(message: dict[str, Any], user_id: int) -> tuple[bool, str | None]:
    attachments = [
        raw for raw in (message.get("attachments") or [])
        if isinstance(raw, dict) and raw.get("type") in {"photo", "video"}
    ]
    if not attachments:
        return False, None

    refs: list[str] = []
    for raw in attachments:
        kind = raw.get("type")
        item = raw.get(kind) if isinstance(raw.get(kind), dict) else {}
        if kind not in {"photo", "video"} or not item:
            continue
        ref = f"{kind}{item.get('owner_id')}_{item.get('id')}"
        if item.get("access_key"):
            ref += f"_{item.get('access_key')}"
        refs.append(ref)
    if not refs:
        return False, None

    saved_files = save_photo_files_from_attachments(
        BASE_DIR,
        attachments,
        prefix=f"jobs_broadcast_edit_{int(time.time() * 1000)}",
        allowed_kinds={"photo", "video"},
        logger=logger,
    )

    with _runtime_data_lock:
        current = runtime_data.get("jobs_broadcast_message", {})
        if not isinstance(current, dict) or not current.get("enabled"):
            return False, None
        block = dict(current)
        current_refs = [part.strip() for part in str(block.get("photo") or "").split(",") if part.strip()]
        current_files = normalize_photo_files(block.get("photo_files"))
        for idx, ref in enumerate(refs):
            if ref not in current_refs:
                current_refs.append(ref)
                current_files.append(saved_files[idx] if idx < len(saved_files) else None)
        block["photo"] = ",".join(current_refs) if current_refs else None
        block["photo_files"] = current_files
        block["updated_at"] = now_msk_str()
        block["updated_by"] = int(user_id)
        runtime_data["jobs_broadcast_message"] = block
        save_runtime_data()
    return True, ",".join(refs)


def handle_jobs_broadcast_edit_input(chat_id: int, user_id: int, text: str, message: dict[str, Any]) -> bool:
    state = USER_STATES.get(str(user_id)) or {}
    pending = state.get("admin_jobs_broadcast_edit")
    if not isinstance(pending, dict) or int(pending.get("chat_id") or 0) != int(chat_id):
        return False
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        state.pop("admin_jobs_broadcast_edit", None)
        send_message(chat_id, "❌ Права администратора больше не подтверждены. Редактирование отменено.")
        return True

    mode = str(pending.get("mode") or "")
    if text.strip().lower() == ".отмена":
        state.pop("admin_jobs_broadcast_edit", None)
        send_message(chat_id, "✅ Редактирование блока вакансий отменено.")
        return True

    if mode == "text":
        if not text.strip():
            send_message(chat_id, "❌ Отправьте новый текст обычным сообщением или .отмена для выхода.")
            return True
        if not update_jobs_broadcast_text(text, user_id):
            state.pop("admin_jobs_broadcast_edit", None)
            send_message(chat_id, "❌ Компактный блок вакансий не найден.")
            return True
        state.pop("admin_jobs_broadcast_edit", None)
        block = get_jobs_broadcast_message()
        attachment, _ = resolve_jobs_broadcast_attachment(block)
        send_message(
            chat_id,
            "✅ Текст блока вакансий обновлён.\n\n" + str(block.get("text") or ""),
            attachment=attachment,
            keyboard=build_jobs_broadcast_keyboard(),
        )
        return True

    if mode == "photo":
        ok, new_refs = add_jobs_broadcast_media(message, user_id)
        if not ok:
            send_message(chat_id, "❌ Прикрепите фото или видео следующим сообщением или напишите .отмена.")
            return True
        state.pop("admin_jobs_broadcast_edit", None)
        block = get_jobs_broadcast_message()
        attachment, _ = resolve_jobs_broadcast_attachment(block)
        send_message(chat_id, "✅ Фото/видео добавлено к блоку вакансий. Старые вложения сохранены.", attachment=attachment)
        return True

    state.pop("admin_jobs_broadcast_edit", None)
    return False


def handle_jobs_broadcast_edit_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    command = str(payload.get("command") or "")
    valid = {
        "edit_jobs_broadcast_text",
        "remove_jobs_broadcast_photo",
        "add_jobs_broadcast_photo",
        "exit_jobs_broadcast_edit",
    }
    if command not in valid:
        return False

    event_object = payload.get("_event_object")
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Нет прав.")
        send_message(chat_id, "❌ Команда доступна только администраторам в админ-чате.")
        return True

    requester_id = int(payload.get("requester_id") or 0)
    if requester_id and requester_id != int(user_id):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Эти кнопки не для вас.")
        send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал .редвак.")
        return True

    block = get_jobs_broadcast_message()
    if command != "exit_jobs_broadcast_edit" and (not block.get("enabled") or not str(block.get("text") or "").strip()):
        send_message(chat_id, "❌ Компактный блок вакансий не настроен. Сначала используйте .добвакрассылка <текст>.")
        return True

    if command == "edit_jobs_broadcast_text":
        USER_STATES.setdefault(str(user_id), {})["admin_jobs_broadcast_edit"] = {
            "mode": "text",
            "chat_id": int(chat_id),
        }
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Жду новый текст.")
        send_message(chat_id, "✏️ Отправьте новый текст блока вакансий следующим сообщением.\n.отмена — отменить редактирование")
        return True

    if command == "remove_jobs_broadcast_photo":
        if remove_jobs_broadcast_photo(user_id):
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Фото удалено.")
            send_message(chat_id, "✅ Все фото/видео из блока вакансий удалены.")
        else:
            send_message(chat_id, "❌ Не удалось удалить фото: блок вакансий не найден.")
        return True

    if command == "add_jobs_broadcast_photo":
        USER_STATES.setdefault(str(user_id), {})["admin_jobs_broadcast_edit"] = {
            "mode": "photo",
            "chat_id": int(chat_id),
        }
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Жду фото.")
        send_message(chat_id, "📎 Прикрепите фото или видео следующим сообщением. Можно отправить несколько сразу.\n.отмена — отменить редактирование")
        return True

    if command == "exit_jobs_broadcast_edit":
        USER_STATES.setdefault(str(user_id), {}).pop("admin_jobs_broadcast_edit", None)
        preview_message_id = int(payload.get("preview_message_id") or 0)
        if preview_message_id:
            delete_chat_message(chat_id, preview_message_id)
        if isinstance(event_object, dict):
            cmid = int(event_object.get("conversation_message_id") or 0)
            if cmid:
                delete_chat_message_by_cmid(chat_id, cmid)
        return True

    return True


def build_telegram_jobs_broadcast_item() -> dict[str, Any] | None:
    block = get_jobs_broadcast_message()
    if not block.get("enabled") or not str(block.get("text") or "").strip():
        return None
    return {
        "text": str(block.get("text") or ""),
        "photo_refs": str(block.get("photo") or ""),
        "photo_files": _telegram_media_files(block.get("photo_files")),
        "button_title": "Посмотреть вакансии",
        "button_link": build_pm_jobs_catalog_deep_link(),
        "delay_after": float(interval_sec or 0),
    }



def _load_broadcast_stats() -> dict[str, Any]:
    with _broadcast_stats_lock:
        data = read_json(BROADCAST_STATS_PATH, {"version": 1, "runs": []})
        if not isinstance(data, dict):
            data = {"version": 1, "runs": []}
        runs = data.get("runs")
        if not isinstance(runs, list):
            data["runs"] = []
        data.setdefault("version", 1)
        return data


def _save_broadcast_stats(data: dict[str, Any]) -> None:
    with _broadcast_stats_lock:
        runs = data.get("runs")
        if not isinstance(runs, list):
            runs = []

        # Храним 90 дней, чтобы файл не разрастался бесконечно.
        cutoff = time.time() - BROADCAST_STATS_KEEP_DAYS * 86400
        filtered: list[dict[str, Any]] = []
        for run in runs:
            if not isinstance(run, dict):
                continue
            try:
                started_ts = float(run.get("started_ts") or 0)
            except (TypeError, ValueError):
                started_ts = 0
            if started_ts <= 0 or started_ts >= cutoff:
                filtered.append(run)

        data["runs"] = filtered[-1500:]
        write_json_atomic(BROADCAST_STATS_PATH, data)


def _stats_item_kind(item: dict[str, Any]) -> str:
    kind = str(item.get("_stats_kind") or "").strip().lower()
    return "main" if kind == "main" else "additional"


def _stats_begin_run(
    *,
    source: str,
    target_chats: list[int],
    items: list[dict[str, Any]],
) -> str:
    now = now_msk()
    run_id = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"

    main_planned = sum(1 for item in items if _stats_item_kind(item) == "main")
    additional_planned = sum(1 for item in items if _stats_item_kind(item) == "additional")

    run = {
        "id": run_id,
        "source": str(source),
        "started_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "started_ts": time.time(),
        "finished_at": None,
        "finished_ts": None,
        "status": "running",
        "target_chats": len(target_chats),
        "sent_chats": 0,
        "failed_chats": 0,
        "planned_main_per_chat": main_planned,
        "planned_additional_per_chat": additional_planned,
        "main_views": 0,
        "additional_views": 0,
        "total_views": 0,
        "reader_reach": 0,
        "messages_sent": 0,
        "last_checked_at": None,
        "chats": {},
    }

    data = _load_broadcast_stats()
    data.setdefault("runs", []).append(run)
    _save_broadcast_stats(data)
    logger.info(
        "Статистика рассылки: создан run=%s, чатов=%s, main=%s, additional=%s",
        run_id,
        len(target_chats),
        main_planned,
        additional_planned,
    )
    return run_id


def _extract_sent_message_id(result: Any) -> int:
    if isinstance(result, bool):
        return 0
    if isinstance(result, int):
        return int(result)
    if isinstance(result, str) and result.isdigit():
        return int(result)
    if isinstance(result, dict):
        for key in ("message_id", "id"):
            try:
                value = int(result.get(key) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                return value
    return 0


def _stats_recalculate_run(run: dict[str, Any]) -> None:
    chats = run.get("chats")
    if not isinstance(chats, dict):
        chats = {}

    main_views = 0
    additional_views = 0
    reader_reach = 0
    messages_sent = 0

    for chat_data in chats.values():
        if not isinstance(chat_data, dict):
            continue
        try:
            readers = max(0, int(chat_data.get("readers") or 0))
        except (TypeError, ValueError):
            readers = 0
        try:
            main_sent = max(0, int(chat_data.get("main_sent") or 0))
        except (TypeError, ValueError):
            main_sent = 0
        try:
            additional_sent = max(0, int(chat_data.get("additional_sent") or 0))
        except (TypeError, ValueError):
            additional_sent = 0

        main_views += readers * main_sent
        additional_views += readers * additional_sent
        reader_reach += readers
        messages_sent += main_sent + additional_sent

    run["main_views"] = int(main_views)
    run["additional_views"] = int(additional_views)
    run["total_views"] = int(main_views + additional_views)
    run["reader_reach"] = int(reader_reach)
    run["messages_sent"] = int(messages_sent)


def _stats_record_chat_delivery(
    run_id: str | None,
    chat_id: int,
    *,
    main_sent: int,
    additional_sent: int,
    last_message_id: int = 0,
) -> None:
    if not run_id:
        return

    data = _load_broadcast_stats()
    changed = False

    for run in reversed(data.get("runs") or []):
        if not isinstance(run, dict) or str(run.get("id")) != str(run_id):
            continue

        chats = run.setdefault("chats", {})
        entry = chats.setdefault(str(int(chat_id)), {})
        entry["chat_id"] = int(chat_id)
        entry["main_sent"] = max(
            int(entry.get("main_sent") or 0),
            int(main_sent or 0),
        )
        entry["additional_sent"] = max(
            int(entry.get("additional_sent") or 0),
            int(additional_sent or 0),
        )
        if int(last_message_id or 0) > 0:
            entry["last_message_id"] = int(last_message_id)
        entry.setdefault("readers", 0)
        entry["delivered_at"] = now_msk_str()
        changed = True
        _stats_recalculate_run(run)
        break

    if changed:
        _save_broadcast_stats(data)


def _stats_finish_run(run_id: str | None, *, sent_chats: int, failed_chats: int) -> None:
    if not run_id:
        return

    data = _load_broadcast_stats()
    for run in reversed(data.get("runs") or []):
        if not isinstance(run, dict) or str(run.get("id")) != str(run_id):
            continue
        run["status"] = "finished"
        run["finished_at"] = now_msk_str()
        run["finished_ts"] = time.time()
        run["sent_chats"] = int(sent_chats)
        run["failed_chats"] = int(failed_chats)
        _stats_recalculate_run(run)
        break
    _save_broadcast_stats(data)


def _chunks(values: list[int], size: int) -> list[list[int]]:
    size = max(1, int(size))
    return [values[i:i + size] for i in range(0, len(values), size)]


def _fetch_conversation_readers(peer_ids: list[int]) -> dict[int, dict[str, int]]:
    """Получает число прочитавших из conversation.out_read_by.

    Админ-права в беседе не требуются: нужен доступ сообщества к самой беседе.
    """
    result: dict[int, dict[str, int]] = {}
    if not peer_ids:
        return result

    client = get_bulk_vk()

    for batch in _chunks(sorted(set(int(x) for x in peer_ids)), 90):
        try:
            response = client.messages.getConversationsById(
                peer_ids=",".join(str(x) for x in batch),
                group_id=int(group_id),
            )
        except Exception as exc:
            logger.warning("Не удалось получить read-статистику бесед %s: %s", batch[:3], exc)
            continue

        for item in response.get("items") or []:
            if not isinstance(item, dict):
                continue

            conversation = item.get("conversation") if isinstance(item.get("conversation"), dict) else item
            if not isinstance(conversation, dict):
                continue

            peer = conversation.get("peer") if isinstance(conversation.get("peer"), dict) else {}
            try:
                peer_id = int(peer.get("id") or 0)
            except (TypeError, ValueError):
                peer_id = 0
            if peer_id <= 0:
                continue

            out_read_by = conversation.get("out_read_by")
            readers = 0
            if isinstance(out_read_by, dict):
                try:
                    readers = max(0, int(out_read_by.get("count") or 0))
                except (TypeError, ValueError):
                    readers = 0
                member_ids = out_read_by.get("member_ids")
                if isinstance(member_ids, list):
                    readers = max(readers, len(member_ids))

            chat_settings = conversation.get("chat_settings")
            members_count = 0
            if isinstance(chat_settings, dict):
                try:
                    members_count = max(0, int(chat_settings.get("members_count") or 0))
                except (TypeError, ValueError):
                    members_count = 0

            try:
                out_read = max(0, int(conversation.get("out_read") or 0))
            except (TypeError, ValueError):
                out_read = 0

            result[peer_id] = {
                "readers": readers,
                "members_count": members_count,
                "out_read": out_read,
            }

    return result


def refresh_broadcast_read_stats(
    *,
    run_id: str | None = None,
    live_hours: int = BROADCAST_STATS_LIVE_HOURS,
) -> int:
    """Обновляет охват по отметкам прочтения VK.

    Это не поле UI `views`: VK API его для message не отдаёт.
    В качестве доступной метрики используем conversation.out_read_by.
    """
    data = _load_broadcast_stats()
    runs = data.get("runs") or []
    now_ts = time.time()
    selected: list[dict[str, Any]] = []

    for run in runs:
        if not isinstance(run, dict):
            continue
        if run_id and str(run.get("id")) != str(run_id):
            continue
        try:
            started_ts = float(run.get("started_ts") or 0)
        except (TypeError, ValueError):
            started_ts = 0
        if not run_id and started_ts and now_ts - started_ts > live_hours * 3600:
            continue
        selected.append(run)

    peer_ids: list[int] = []
    for run in selected:
        chats = run.get("chats")
        if not isinstance(chats, dict):
            continue
        for chat_key in chats:
            try:
                peer_ids.append(int(chat_key))
            except (TypeError, ValueError):
                pass

    current = _fetch_conversation_readers(peer_ids)
    if not current:
        return 0

    changed = 0
    data = _load_broadcast_stats()
    selected_ids = {str(run.get("id")) for run in selected}

    for run in data.get("runs") or []:
        if not isinstance(run, dict) or str(run.get("id")) not in selected_ids:
            continue

        chats = run.get("chats")
        if not isinstance(chats, dict):
            continue

        for chat_key, chat_data in chats.items():
            if not isinstance(chat_data, dict):
                continue
            try:
                peer_id = int(chat_key)
            except (TypeError, ValueError):
                continue

            fresh = current.get(peer_id)
            if not fresh:
                continue

            old_readers = max(0, int(chat_data.get("readers") or 0))
            new_readers = max(old_readers, int(fresh.get("readers") or 0))

            # Не считаем само сообщество участником просмотра.
            members_count = max(0, int(fresh.get("members_count") or 0))
            if members_count > 0:
                new_readers = min(new_readers, members_count)

            if new_readers != old_readers:
                chat_data["readers"] = new_readers
                changed += 1

            chat_data["members_count"] = members_count
            chat_data["out_read"] = int(fresh.get("out_read") or 0)
            chat_data["last_checked_at"] = now_msk_str()

        run["last_checked_at"] = now_msk_str()
        _stats_recalculate_run(run)

    _save_broadcast_stats(data)
    return changed


def broadcast_stats_monitor_loop() -> None:
    while True:
        try:
            refresh_broadcast_read_stats()
        except Exception as exc:
            logger.warning("Ошибка фонового обновления статистики просмотров: %s", exc)
        time.sleep(BROADCAST_STATS_REFRESH_SECONDS)


def _period_start_ts(period: str) -> float | None:
    current = now_msk()
    if period == "day":
        return current.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    if period == "week":
        return (current - timedelta(days=7)).timestamp()
    if period == "month":
        return (current - timedelta(days=30)).timestamp()
    return None


def _stats_runs_for_period(period: str) -> list[dict[str, Any]]:
    data = _load_broadcast_stats()
    runs = [run for run in data.get("runs") or [] if isinstance(run, dict)]
    runs.sort(key=lambda r: float(r.get("started_ts") or 0))

    if period == "latest":
        return runs[-1:] if runs else []

    start_ts = _period_start_ts(period)
    if start_ts is None:
        return runs

    return [
        run for run in runs
        if float(run.get("started_ts") or 0) >= start_ts
    ]


def _format_number(value: Any) -> str:
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "0"


def _panel_period_title(period: str) -> str:
    return {
        "latest": "Последняя рассылка",
        "day": "Сегодня",
        "week": "Последние 7 дней",
        "month": "Последние 30 дней",
        "chats": "Чаты последней рассылки",
    }.get(period, "Последняя рассылка")


def build_stats_panel_keyboard(period: str = "latest") -> str:
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(
        "📊 Последняя",
        color=VkKeyboardColor.PRIMARY if period == "latest" else VkKeyboardColor.SECONDARY,
        payload={"command": "stats_panel_latest"},
    )
    keyboard.add_callback_button(
        "☀️ Сегодня",
        color=VkKeyboardColor.PRIMARY if period == "day" else VkKeyboardColor.SECONDARY,
        payload={"command": "stats_panel_day"},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📅 7 дней",
        color=VkKeyboardColor.PRIMARY if period == "week" else VkKeyboardColor.SECONDARY,
        payload={"command": "stats_panel_week"},
    )
    keyboard.add_callback_button(
        "🗓 30 дней",
        color=VkKeyboardColor.PRIMARY if period == "month" else VkKeyboardColor.SECONDARY,
        payload={"command": "stats_panel_month"},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📍 По чатам",
        color=VkKeyboardColor.PRIMARY if period == "chats" else VkKeyboardColor.SECONDARY,
        payload={"command": "stats_panel_chats"},
    )
    keyboard.add_callback_button(
        "🔄 Обновить",
        color=VkKeyboardColor.POSITIVE,
        payload={"command": f"stats_panel_refresh_{period}"},
    )
    return keyboard.get_keyboard()


def _broadcast_runtime_summary() -> tuple[int, int, str, str, str]:
    with _chat_ids_lock:
        chats_count = len([
            cid for cid in list(chat_ids)
            if admin_chat is not None and cid != admin_chat
            and len(str(cid)) == 10 and str(cid).startswith("2")
        ])

    try:
        active_ads = len(get_active_random_orders())
    except Exception:
        active_ads = 0

    broadcast_state = "🟢 Идёт" if broadcast_thread and broadcast_thread.is_alive() else "⚪️ Ожидание"

    media_ref = upload_message_photo()
    if isinstance(media_ref, str) and media_ref.startswith("video"):
        media_state = "🎬 Видео"
    elif isinstance(media_ref, str) and media_ref.startswith("photo"):
        media_state = "🖼 Фото"
    elif media_ref:
        media_state = "📎 Вложение"
    else:
        media_state = "— Нет"

    try:
        next_ts = float(last_broadcast_time) + max(float(cd_min), 0.0) * 60
        next_dt = datetime.fromtimestamp(next_ts, tz=MSK)
        next_text = next_dt.strftime("%d.%m %H:%M")
    except Exception:
        next_text = "—"

    return chats_count, active_ads, broadcast_state, media_state, next_text


def render_stats_panel(period: str = "latest") -> str:
    runs = _stats_runs_for_period(period if period != "chats" else "latest")
    chats_count, active_ads, broadcast_state, media_state, next_text = _broadcast_runtime_summary()

    if period == "chats":
        if not runs:
            details = "Пока нет сохранённых рассылок."
        else:
            run = runs[-1]
            chat_rows: list[tuple[int, int, str]] = []
            registry = get_chat_registry()
            for chat_key, chat_data in (run.get("chats") or {}).items():
                if not isinstance(chat_data, dict):
                    continue
                try:
                    chat_id = int(chat_key)
                    readers = int(chat_data.get("readers") or 0)
                except (TypeError, ValueError):
                    continue
                title = str((registry.get(str(chat_id)) or {}).get("title") or f"Чат {chat_id}")
                chat_rows.append((readers, chat_id, title))

            chat_rows.sort(reverse=True)
            lines = []
            for index, (readers, chat_id, title) in enumerate(chat_rows[:12], start=1):
                lines.append(f"{index}. 👁 {readers} — {title[:34]}")
            details = "\n".join(lines) if lines else "Нет данных по чатам."

        return (
            "📊 ПАНЕЛЬ БОТА\n"
            "━━━━━━━━━━━━━━\n"
            f"💬 Чатов в рассылке: {chats_count}\n"
            f"📨 Активных доп. сообщений: {active_ads}\n"
            f"📡 Рассылка: {broadcast_state}\n"
            f"🧩 Основное медиа: {media_state}\n"
            f"⏭ Следующая: {next_text} МСК\n\n"
            f"📍 {_panel_period_title(period)}\n"
            f"{details}\n\n"
            "ℹ️ 👁 считается по отметкам прочтения VK."
        )

    main_views = sum(int(run.get("main_views") or 0) for run in runs)
    additional_views = sum(int(run.get("additional_views") or 0) for run in runs)
    total_views = main_views + additional_views
    reader_reach = sum(int(run.get("reader_reach") or 0) for run in runs)
    messages_sent = sum(int(run.get("messages_sent") or 0) for run in runs)
    sent_chats = sum(int(run.get("sent_chats") or 0) for run in runs)
    failed_chats = sum(int(run.get("failed_chats") or 0) for run in runs)

    last_run = runs[-1] if runs else None
    if last_run:
        last_started = str(last_run.get("started_at") or "—")
        last_checked = str(last_run.get("last_checked_at") or "ещё не обновлялось")
    else:
        last_started = "—"
        last_checked = "—"

    return (
        "📊 ПАНЕЛЬ БОТА\n"
        "━━━━━━━━━━━━━━\n"
        f"💬 Чатов в рассылке: {chats_count}\n"
        f"📨 Активных доп. сообщений: {active_ads}\n"
        f"📡 Рассылка: {broadcast_state}\n"
        f"🧩 Основное медиа: {media_state}\n"
        f"⏭ Следующая: {next_text} МСК\n\n"
        f"📈 {_panel_period_title(period)}\n"
        f"👁 Основное сообщение: {_format_number(main_views)}\n"
        f"👁 Дополнительные сообщения: {_format_number(additional_views)}\n"
        f"🔥 Всего просмотров: {_format_number(total_views)}\n"
        f"👥 Суммарный охват чтения: {_format_number(reader_reach)}\n\n"
        f"📤 Отправлено сообщений: {_format_number(messages_sent)}\n"
        f"✅ Доставок в чаты: {_format_number(sent_chats)}\n"
        f"⚠️ Ошибок чатов: {_format_number(failed_chats)}\n"
        f"🧾 Рассылок в периоде: {_format_number(len(runs))}\n\n"
        f"🕒 Последняя: {last_started}\n"
        f"🔄 Данные: {last_checked}\n\n"
        "ℹ️ VK API не отдаёт отдельное поле views у сообщений. "
        "Поэтому 👁 здесь — доступный счётчик прочтений из out_read_by; "
        "он работает и без админ-прав бота в беседе, если сообщество имеет доступ к чату."
    )


def _edit_stats_panel_from_event(
    chat_id: int,
    text_value: str,
    keyboard: str,
    event_object: dict[str, Any] | None,
) -> bool:
    if not isinstance(event_object, dict):
        return False

    try:
        cmid = int(event_object.get("conversation_message_id") or 0)
    except (TypeError, ValueError):
        cmid = 0
    if cmid <= 0:
        return False

    try:
        vk.messages.edit(
            peer_id=int(chat_id),
            conversation_message_id=cmid,
            message=text_value,
            keyboard=keyboard,
        )
        return True
    except Exception as exc:
        logger.debug("Не удалось обновить панель через messages.edit: %s", exc)
        return False


def show_stats_panel(
    chat_id: int,
    user_id: int,
    *,
    period: str = "latest",
    event_object: dict[str, Any] | None = None,
    force_refresh: bool = False,
) -> None:
    if not has_permission(user_id, "admin"):
        send_message(chat_id, "❌ Панель доступна только администрации бота.")
        return
    if chat_id != admin_chat:
        send_message(chat_id, "❌ Панель доступна только в админ-чате.")
        return

    if force_refresh:
        try:
            refresh_broadcast_read_stats()
        except Exception as exc:
            logger.warning("Не удалось вручную обновить статистику: %s", exc)

    text_value = render_stats_panel(period)
    keyboard = build_stats_panel_keyboard(period)

    if event_object:
        answer_callback_event(event_object, "Статистика обновлена")
        if _edit_stats_panel_from_event(chat_id, text_value, keyboard, event_object):
            return

    send_message(chat_id, text_value, keyboard=keyboard, rate_limit=False)


def handle_stats_panel_action(
    chat_id: int,
    user_id: int,
    payload: dict[str, Any],
) -> bool:
    command = str(payload.get("command") or "").strip().lower()

    mapping = {
        "stats_panel_latest": "latest",
        "stats_panel_day": "day",
        "stats_panel_week": "week",
        "stats_panel_month": "month",
        "stats_panel_chats": "chats",
    }

    if command in mapping:
        show_stats_panel(
            chat_id,
            user_id,
            period=mapping[command],
            event_object=payload.get("_event_object"),
        )
        return True

    prefix = "stats_panel_refresh_"
    if command.startswith(prefix):
        period = command[len(prefix):] or "latest"
        if period not in {"latest", "day", "week", "month", "chats"}:
            period = "latest"
        show_stats_panel(
            chat_id,
            user_id,
            period=period,
            event_object=payload.get("_event_object"),
            force_refresh=True,
        )
        return True

    return False


def build_vk_full_broadcast_items() -> list[dict[str, Any]]:
    """Готовит текст, фото и кнопки один раз перед запуском потоков.

    Раньше каждый чат заново читал ads.json, восстанавливал фотографии и
    менял глобальный ads_data. При нескольких чатах это создавало лишнюю
    нагрузку и гонки записи. Теперь фоновые потоки только отправляют готовый
    неизменяемый снимок рассылки.
    """
    global ads_data
    ads_data = order_service.load_ads()
    items: list[dict[str, Any]] = []
    ads_changed = False

    for ad_key, ad in get_active_random_orders():
        safety = detect_military_recruitment(ad)
        if safety.get("blocked"):
            delete_ad_for_safety(ad_key, ad, safety, notify=True)
            ads_changed = True
            continue
        attachment, changed = resolve_ad_photo_attachment(ad, str(ad.get("ad_key") or ad_key))
        ads_changed = ads_changed or changed
        items.append({
            "text": str(ad.get("text") or ""),
            "attachment": attachment,
            "keyboard": build_ad_openlink_keyboard(ad),
            "delay_after": float(interval_sec or 0),
            "_stats_kind": "additional",
        })

    # Отдельный компактный блок вакансий идёт непосредственно перед старым
    # основным сообщением. Благодаря ему не нужно добавлять 10–15 вакансий как
    # отдельные заказы в общую рассылку: одна кнопка ведёт во весь каталог ЛС.
    jobs_block = get_jobs_broadcast_message()
    if jobs_block.get("enabled") and str(jobs_block.get("text") or "").strip():
        jobs_attachment, jobs_changed = resolve_jobs_broadcast_attachment(jobs_block)
        if jobs_changed:
            with _runtime_data_lock:
                runtime_data["jobs_broadcast_message"] = jobs_block
                save_runtime_data()
        items.append({
            "text": str(jobs_block.get("text") or ""),
            "attachment": jobs_attachment,
            "keyboard": build_jobs_broadcast_keyboard(),
            "delay_after": float(interval_sec or 0),
            "_stats_kind": "additional",
        })

    if message_text:
        items.append({
            "text": str(message_text or ""),
            "attachment": upload_message_photo(),
            "keyboard": build_buy_ad_keyboard(),
            "delay_after": float(interval_sec or 0),
            "_stats_kind": "main",
        })

    main_text_sale = ads_data.get("main_text_sale", {})
    if main_text_sale and main_text_sale.get("status") == "active" and not main_text_sale.get("broadcast_paused"):
        expires_at = main_text_sale.get("expires_at")
        is_active = True
        if expires_at:
            try:
                is_active = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK) > now_msk()
            except ValueError:
                is_active = True
        if is_active:
            safety = detect_military_recruitment(main_text_sale)
            if safety.get("blocked"):
                delete_ad_for_safety("main_text_sale", main_text_sale, safety, notify=True)
                ads_changed = True
                is_active = False
        if is_active:
            attachment, changed = resolve_ad_photo_attachment(main_text_sale, "main_text_sale")
            ads_changed = ads_changed or changed
            items.append({
                "text": str(main_text_sale.get("text") or ""),
                "attachment": attachment,
                "keyboard": build_ad_openlink_keyboard(main_text_sale),
                "delay_after": float(interval_sec or 0),
                "_stats_kind": "additional",
            })

    if ads_changed:
        save_ads()
    return items



def _send_main_video_message_strict(
    chat_id: int,
    text_value: str,
    attachment: str,
    keyboard: str | None = None,
    *,
    api_client: Any | None = None,
) -> Any:
    """Основное видео нельзя тихо заменять сообщением только с текстом."""
    client = api_client or get_bulk_vk()
    params: dict[str, Any] = {
        "peer_id": int(chat_id),
        "message": str(text_value or " "),
        "attachment": str(attachment),
        "random_id": generate_random_id(),
    }
    if keyboard:
        params["keyboard"] = keyboard

    try:
        return client.messages.send(**params)
    except Exception as exc:
        logger.error(
            "Основное видео НЕ отправлено в чат %s, attachment=%s: %s",
            chat_id,
            attachment,
            exc,
        )
        return None



def _history_message_has_video(
    client: Any,
    chat_id: int,
    sent_id: Any | None = None,
) -> bool | None:
    """Проверяет последнее исходящее сообщение через getHistory.

    Возвращает:
      True  — видео точно есть;
      False — сообщение найдено, видео точно нет;
      None  — проверить не удалось.
    """
    try:
        response = client.messages.getHistory(
            peer_id=int(chat_id),
            count=5,
            group_id=int(group_id),
        )
        items = response.get("items") or []
    except Exception as exc:
        logger.debug("Не удалось проверить историю peer=%s: %s", chat_id, exc)
        return None

    sent_id_str = str(sent_id) if sent_id is not None else ""

    for item in items:
        if not isinstance(item, dict):
            continue

        item_id = str(item.get("id") or "")
        if sent_id_str and item_id and item_id != sent_id_str:
            continue

        return _message_has_video(item)

    # Если по ID сообщение не нашли, проверку считаем неопределённой.
    return None


def _delete_failed_probe_message(client: Any, sent_id: Any) -> None:
    if not sent_id:
        return
    try:
        client.messages.delete(
            message_ids=str(sent_id),
            delete_for_all=1,
        )
    except Exception:
        pass


def _sent_message_contains_video(client: Any, sent_id: Any) -> bool | None:
    """Проверяет отправленное сообщение корректно для group token."""
    if sent_id is None:
        return None
    try:
        response = client.messages.getById(
            message_ids=str(sent_id),
            group_id=int(group_id),
        )
        items = response.get("items") or []
        if not items:
            return None
        return _message_has_video(items[0])
    except Exception as exc:
        logger.debug("Не удалось проверить отправленное видео id=%s: %s", sent_id, exc)
        return None


def _send_main_message_video_only(
    chat_id: int,
    text_value: str,
    keyboard: str | None = None,
    *,
    preferred_attachment: str | None = None,
    api_client: Any | None = None,
) -> Any:
    """Простая отправка: video attachment -> крайний text-only fallback.

    В целевой чат основной текст никогда не отправляется дважды.
    """
    client = api_client or get_bulk_vk()

    video_ref = None
    for candidate in (
        preferred_attachment,
        message_photo_attachment,
        message_photo_path if isinstance(message_photo_path, str) else None,
    ):
        ready = str(candidate or "").strip()
        if ready.startswith("video"):
            video_ref = ready
            break

    if video_ref:
        params: dict[str, Any] = {
            "peer_id": int(chat_id),
            "message": str(text_value or " "),
            "attachment": video_ref,
            "random_id": generate_random_id(),
        }
        if keyboard:
            params["keyboard"] = keyboard

        try:
            result = client.messages.send(**params)
            logger.info("MAIN VIDEO SENT peer=%s attachment=%s", chat_id, video_ref)
            return result
        except Exception as exc:
            logger.error(
                "MAIN VIDEO ERROR peer=%s attachment=%s: %s",
                chat_id,
                video_ref,
                exc,
            )

    # Крайний случай — одна отправка текста без медиа.
    params = {
        "peer_id": int(chat_id),
        "message": str(text_value or " "),
        "random_id": generate_random_id(),
    }
    if keyboard:
        params["keyboard"] = keyboard

    try:
        logger.error("Основное видео недоступно peer=%s — text-only fallback", chat_id)
        return client.messages.send(**params)
    except Exception as exc:
        logger.error("Не отправился основной текст peer=%s: %s", chat_id, exc)
        return None


# Оставляем старое имя для совместимости с уже вызывающим кодом.
def _send_main_message_with_media_guaranteed(
    chat_id: int,
    text_value: str,
    keyboard: str | None = None,
    *,
    preferred_attachment: str | None = None,
    api_client: Any | None = None,
) -> Any:
    return _send_main_message_video_only(
        chat_id,
        text_value,
        keyboard,
        preferred_attachment=preferred_attachment,
        api_client=api_client,
    )


def send_broadcast_to_chat(chat_id: int, items: list[dict[str, Any]], stats_run_id: str | None = None) -> str | None:
    """Отправляет готовый снимок рассылки через отдельный HTTP-клиент."""
    api_client = get_bulk_vk()
    logger.info(f"Рассылка: отправка в чат {chat_id}")
    stats_main_sent = 0
    stats_additional_sent = 0
    stats_last_message_id = 0
    for item in items:
        item_text = str(item.get("text") or "")
        item_attachment = str(item.get("attachment") or "").strip() or None

        # Для video attachment используем ту же основную VK-сессию,
        # как в старом рабочем коде. Никаких проверок, пересылок и документов.
        if item_attachment and item_attachment.startswith("video"):
            params: dict[str, Any] = {
                "peer_id": int(chat_id),
                "message": item_text or " ",
                "attachment": item_attachment,
                "random_id": generate_random_id(),
            }
            if item.get("keyboard"):
                params["keyboard"] = item.get("keyboard")
            try:
                result = vk.messages.send(**params)
                logger.info(
                    "Основное/обычное VK-видео отправлено старым способом peer=%s attachment=%s",
                    chat_id,
                    item_attachment,
                )
            except Exception as exc:
                logger.error(
                    "Ошибка прямой отправки VK-видео peer=%s attachment=%s: %s",
                    chat_id,
                    item_attachment,
                    exc,
                )
                # Только если сам VK вернул ошибку — один раз отправляем текст.
                result = send_message(
                    chat_id,
                    item_text,
                    keyboard=item.get("keyboard"),
                    rate_limit=False,
                    api_client=api_client,
                )
        else:
            result = send_message(
                chat_id,
                item_text,
                attachment=item_attachment,
                keyboard=item.get("keyboard"),
                rate_limit=False,
                api_client=api_client,
            )

        if result == "access_error":
            _stats_record_chat_delivery(
                stats_run_id,
                chat_id,
                main_sent=stats_main_sent,
                additional_sent=stats_additional_sent,
                last_message_id=stats_last_message_id,
            )
            logger.warning(f"Рассылка остановлена: потерян доступ к чату {chat_id}")
            return "access_error"

        if result is not None:
            if _stats_item_kind(item) == "main":
                stats_main_sent += 1
            else:
                stats_additional_sent += 1
            sent_message_id = _extract_sent_message_id(result)
            if sent_message_id > 0:
                stats_last_message_id = sent_message_id
        delay_after = max(0.0, float(item.get("delay_after", 0) or 0))
        if delay_after:
            time.sleep(delay_after)

    _stats_record_chat_delivery(
        stats_run_id,
        chat_id,
        main_sent=stats_main_sent,
        additional_sent=stats_additional_sent,
        last_message_id=stats_last_message_id,
    )
    return None


def iter_active_ads() -> list[tuple[str, dict[str, Any]]]:
    current_ads = order_service.load_ads()
    result: list[tuple[str, dict[str, Any]]] = []
    for key, ad_data in current_ads.items():
        if not isinstance(ad_data, dict):
            continue
        if key in {"active_ad", "users"}:
            continue
        if is_user_mirror_ad_key(key, ad_data):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue
        expires_at = ad_data.get("expires_at")
        if expires_at:
            try:
                if datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK) < now_msk():
                    continue
            except ValueError:
                pass
        result.append((key, ad_data))
    return result


def mark_related_order_expired(order_id: str | None) -> None:
    if not order_id or order_id not in orders_data:
        return
    order = orders_data[order_id]
    if order.get("status") not in {"approved", "active"}:
        return
    timestamp = now_msk_str()
    order["status"] = "deleted"
    order["deleted_at"] = timestamp
    order.setdefault("events", []).append({"at": timestamp, "event": "expired"})


def process_expiring_ads() -> None:
    global ads_data, orders_data
    ads_data = order_service.load_ads()
    orders_data = order_service.load_orders()
    now = now_msk()
    changed = False

    for item_key, ad_data in list(ads_data.items()):
        if item_key in {"active_ad", "users"}:
            continue
        if not isinstance(ad_data, dict):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue

        expires_dt = parse_dt(ad_data.get("expires_at"))
        if not expires_dt:
            continue

        user_id = int(ad_data.get("user_id") or 0)
        is_main = item_key == "main_text_sale" or ad_data.get("type") == "main_text"
        keyboard = build_extend_specific_keyboard(item_key, is_main)
        title = "основная реклама" if is_main else "дополнительная реклама"
        order_code = (
            str(ad_data.get("order_code", "")).upper()
            or get_order_display_code(ad_data.get("order_id"))
            or "без кода"
        )

        remaining = expires_dt - now
        if timedelta(hours=23) <= remaining <= timedelta(days=1, hours=1) and not ad_data.get(EXPIRY_WARNING_DAY):
            safe_send_pm(
                user_id,
                f"⚠️ Напоминание: через 1 день закончится {title}.\nКод заказа: {order_code}\nПродлите её заранее.",
                keyboard=keyboard,
            )
            ad_data[EXPIRY_WARNING_DAY] = now.strftime("%Y-%m-%d %H:%M:%S")
            changed = True

        if timedelta(minutes=30) <= remaining <= timedelta(hours=1, minutes=30) and not ad_data.get(EXPIRY_WARNING_HOUR):
            safe_send_pm(
                user_id,
                f"⏰ Напоминание: через 1 час закончится {title}.\nКод заказа: {order_code}\nНажмите кнопку ниже, чтобы продлить именно это объявление.",
                keyboard=keyboard,
            )
            ad_data[EXPIRY_WARNING_HOUR] = now.strftime("%Y-%m-%d %H:%M:%S")
            changed = True

        if expires_dt <= now and not ad_data.get(EXPIRY_REMOVED_AT):
            safe_send_pm(
                user_id,
                f"❌ Срок рекламы истёк.\n{title.capitalize()} с кодом {order_code} удалена из активных объявлений.",
                keyboard=keyboard,
            )
            ad_data[EXPIRY_REMOVED_AT] = now.strftime("%Y-%m-%d %H:%M:%S")
            ad_data["status"] = "expired"
            mark_related_order_expired(ad_data.get("order_id") or (item_key if item_key in orders_data else None))
            if item_key == "main_text_sale":
                ads_data.pop("main_text_sale", None)
            else:
                ads_data.pop(item_key, None)
                user_key = str(user_id)
                if ads_data.get(user_key) is ad_data:
                    ads_data.pop(user_key, None)
                users_ads = ads_data.get("users")
                if isinstance(users_ads, dict) and users_ads.get(user_key) is ad_data:
                    users_ads.pop(user_key, None)
                active_ad = ads_data.get("active_ad")
                if active_ad is ad_data:
                    ads_data.pop("active_ad", None)
            changed = True

    if changed:
        save_orders()
        save_ads()


def expiration_monitor() -> None:
    while True:
        try:
            process_expiring_ads()
            process_safety_moderation()
        except Exception as exc:
            logger.error(f"Ошибка обработки истечения рекламы: {exc}")
        time.sleep(60)


def render_additional_texts_list() -> str:
    additional_texts = get_additional_texts()
    additional_photos = get_additional_photos()
    if not additional_texts:
        return "Список дополнительных текстов пуст."
    lines = ["📋 Дополнительные тексты:"]
    for index, text_value in enumerate(additional_texts, start=1):
        photo_count = len(additional_photos.get(str(index - 1), []))
        preview = text_value if len(text_value) <= 160 else f"{text_value[:160]}..."
        lines.append(f"{index}. {preview}")
        lines.append(f"   Фото: {photo_count}")
    return "\n".join(lines)


def _resolve_vk_sender_name(sender_id: int) -> str | None:
    """Получает Имя Фамилию пользователя только через API сообщества."""
    now_ts = time.time()

    with _vk_sender_name_cache_lock:
        cached = _vk_sender_name_cache.get(int(sender_id))
        if cached and now_ts - float(cached[1]) < VK_SENDER_NAME_CACHE_TTL_SECONDS:
            return cached[0]

    clients: list[Any] = []
    try:
        clients.append(vk)
    except Exception:
        pass

    try:
        bulk_client = get_bulk_vk()
        if bulk_client not in clients:
            clients.append(bulk_client)
    except Exception:
        pass

    for client in clients:
        try:
            response = client.users.get(user_ids=int(sender_id))
            if not isinstance(response, list) or not response:
                continue

            user = response[0] if isinstance(response[0], dict) else {}
            first_name = str(user.get("first_name") or "").strip()
            last_name = str(user.get("last_name") or "").strip()
            full_name = " ".join(part for part in (first_name, last_name) if part).strip()

            if not full_name:
                continue

            with _vk_sender_name_cache_lock:
                _vk_sender_name_cache[int(sender_id)] = (full_name, now_ts)

            return full_name
        except Exception as exc:
            logger.debug(
                "Не удалось получить имя VK пользователя %s через users.get: %s",
                sender_id,
                exc,
            )

    return None


def _resolve_user_from_conversation(peer_id: int, sender_id: int) -> str | None:
    """Fallback: ищет Имя Фамилию автора среди участников конкретной беседы."""
    clients: list[Any] = []
    try:
        clients.append(vk)
    except Exception:
        pass
    try:
        bulk_client = get_bulk_vk()
        if bulk_client not in clients:
            clients.append(bulk_client)
    except Exception:
        pass

    for client in clients:
        try:
            response = client.messages.getConversationMembers(
                peer_id=int(peer_id),
                fields="first_name,last_name",
            )
            profiles = response.get("profiles") if isinstance(response, dict) else None
            if not isinstance(profiles, list):
                continue

            for profile in profiles:
                if not isinstance(profile, dict):
                    continue
                if int(profile.get("id") or 0) != int(sender_id):
                    continue

                first_name = str(profile.get("first_name") or "").strip()
                last_name = str(profile.get("last_name") or "").strip()
                full_name = " ".join(
                    part for part in (first_name, last_name) if part
                ).strip()
                if not full_name:
                    continue

                with _vk_sender_name_cache_lock:
                    _vk_sender_name_cache[int(sender_id)] = (
                        full_name,
                        time.time(),
                    )

                return full_name
        except Exception as exc:
            logger.debug(
                "Не удалось найти пользователя %s среди участников беседы %s: %s",
                sender_id,
                peer_id,
                exc,
            )

    return None


def render_application_author_mention(peer_id: int, user_id: int) -> str:
    """Надёжное имя автора заявки без отдельного пользовательского аккаунта."""
    try:
        sender_id = int(user_id)
    except (TypeError, ValueError):
        return "Отправитель не определён"

    if sender_id == 0:
        return "Отправитель не определён"

    if sender_id < 0:
        return render_user_mention(sender_id)

    full_name = _resolve_vk_sender_name(sender_id)
    if not full_name:
        full_name = _resolve_user_from_conversation(peer_id, sender_id)

    if full_name:
        return f"[id{sender_id}|{full_name}]"

    logger.warning(
        "Не удалось определить имя автора заявки %s в беседе %s",
        sender_id,
        peer_id,
    )
    return f"[id{sender_id}|Пользователь {sender_id}]"


def render_user_mention(user_id: int) -> str:
    """Возвращает кликабельное реальное имя пользователя/сообщества."""
    client = vk if threading.get_ident() == _main_vk_thread_ident else get_bulk_vk()

    try:
        sender_id = int(user_id)
    except (TypeError, ValueError):
        return str(user_id)

    if sender_id < 0:
        community_id = abs(sender_id)
        try:
            groups = client.groups.getById(group_id=community_id)
            group = groups[0] if isinstance(groups, list) and groups else {}
            name = str(group.get("name") or f"Сообщество {community_id}").strip()
            return f"[club{community_id}|{name}]"
        except Exception:
            return f"[club{community_id}|Сообщество {community_id}]"

    full_name = _resolve_vk_sender_name(sender_id)
    if full_name:
        return f"[id{sender_id}|{full_name}]"

    # Только настоящий API-сбой оставляет запасной вариант с ID.
    logger.warning(
        "Не удалось определить имя/фамилию VK пользователя %s ни через group token, ни через user token",
        sender_id,
    )
    return f"[id{sender_id}|Пользователь {sender_id}]"


def get_order_display_code(order_id: str, order: dict[str, Any] | None = None) -> str:
    order = order or orders_data.get(order_id) or {}
    code = order.get("order_code")
    if code:
        return str(code).upper()
    return order_id


def normalize_order_lookup_code(value: str) -> str:
    return str(value or "").upper().replace("O", "0")


def find_best_order_for_ad(ad_data: dict[str, Any]) -> str | None:
    user_id = ad_data.get("user_id")
    ad_text = (ad_data.get("text") or "").strip()
    ad_type = ad_data.get("type")
    ad_created = ad_data.get("created_at") or ""
    ad_days = int(ad_data.get("days", 0) or 0)
    candidates: list[tuple[str, dict[str, Any]]] = []
    for candidate_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        if order.get("user_id") != user_id:
            continue
        if order.get("status") not in {"approved", "pending"}:
            continue
        if ad_type == "main_text" and order.get("type") != "main_text":
            continue
        if ad_type != "main_text" and order.get("type") == "main_text":
            continue
        if (order.get("text") or "").strip() != ad_text:
            continue
        if int(order.get("days", 0) or 0) != ad_days:
            continue
        candidates.append((candidate_id, order))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[1].get("approved_at") or item[1].get("created_at") or ad_created, reverse=True)
    return candidates[0][0]


def resolve_order_reference(order_ref: str) -> str | None:
    order_ref = str(order_ref or "").strip()
    if order_ref in orders_data:
        return order_ref
    upper_ref = order_ref.upper()
    normalized_ref = normalize_order_lookup_code(order_ref)
    for candidate_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        # Поддерживаем и короткий порядковый номер из списка заказов.
        if str(order.get("order_no") or "") == order_ref:
            return candidate_id
        candidate_code = str(order.get("order_code", "")).upper()
        if candidate_code == upper_ref or normalize_order_lookup_code(candidate_code) == normalized_ref:
            return candidate_id
    current_ads = order_service.load_ads()
    for ad_key, ad_data in current_ads.items():
        if not isinstance(ad_data, dict):
            continue
        if ad_key in {"users", "active_ad"}:
            continue
        if is_user_mirror_ad_key(ad_key, ad_data):
            continue
        ad_code = str(ad_data.get("order_code", "")).upper()
        if ad_code != upper_ref and normalize_order_lookup_code(ad_code) != normalized_ref:
            continue
        related_order_id = ad_data.get("order_id")
        if related_order_id and related_order_id in orders_data:
            return related_order_id
        if ad_key in orders_data:
            return ad_key
        inferred_order_id = find_best_order_for_ad(ad_data)
        if inferred_order_id:
            ad_data["order_id"] = inferred_order_id
            ad_data["order_code"] = orders_data[inferred_order_id].get("order_code")
            ad_data["ad_key"] = ad_key
            ad_data["type"] = ad_data.get("type") or orders_data[inferred_order_id].get("type")
            try:
                order_service.save_ads(current_ads)
            except Exception as exc:
                logger.warning(f"Не удалось сохранить привязку рекламы к заказу: {exc}")
            return inferred_order_id
    return None



def resolve_ad_for_order_reference(order_ref: str) -> tuple[str | None, str | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Возвращает (order_id, ad_key, order, ad) по внутреннему ID, номеру или коду заказа."""
    global ads_data, orders_data
    orders_data = order_service.load_orders()
    ads_data = order_service.load_ads()

    raw_ref = str(order_ref or "").strip()
    if not raw_ref:
        return None, None, None, None
    normalized_ref = normalize_order_lookup_code(raw_ref)

    # Сначала ищем прямо среди активных рекламных записей. Это важно для
    # продлений и обновлений, где код текущего объявления может принадлежать
    # исходному заказу.
    for key, ad_data in ads_data.items():
        if not isinstance(ad_data, dict):
            continue
        if key in {"active_ad", "users"} or is_user_mirror_ad_key(key, ad_data):
            continue
        candidates = {
            str(key),
            str(ad_data.get("ad_key") or ""),
            str(ad_data.get("order_id") or ""),
            str(ad_data.get("order_code") or ""),
        }
        if any(normalize_order_lookup_code(value) == normalized_ref for value in candidates if value):
            linked_order_id = str(ad_data.get("order_id") or "") or resolve_order_reference(raw_ref)
            linked_order = orders_data.get(linked_order_id) if linked_order_id else None
            return linked_order_id or None, str(key), linked_order, ad_data

    order_id = resolve_order_reference(raw_ref)
    if not order_id:
        return None, None, None, None
    order = orders_data.get(order_id)
    if not isinstance(order, dict):
        return order_id, None, order, None

    possible_keys: list[str] = []
    if order.get("type") == "main_text":
        possible_keys.append("main_text_sale")
    for value in (
        order.get("ad_key"),
        order.get("source_order_id"),
        order.get("item_key"),
        order_id,
    ):
        if value:
            possible_keys.append(str(value))

    for key in possible_keys:
        ad_data = ads_data.get(key)
        if isinstance(ad_data, dict):
            return order_id, key, order, ad_data

    # Последний запасной поиск по пользователю и тексту заказа.
    order_user_id = order.get("user_id")
    order_text = str(order.get("text") or "").strip()
    for key, ad_data in ads_data.items():
        if not isinstance(ad_data, dict):
            continue
        if key in {"active_ad", "users"} or is_user_mirror_ad_key(key, ad_data):
            continue
        if order_user_id and ad_data.get("user_id") != order_user_id:
            continue
        if order_text and str(ad_data.get("text") or "").strip() != order_text:
            continue
        return order_id, str(key), order, ad_data

    return order_id, None, order, None


def set_order_broadcast_paused(order_ref: str, paused: bool, admin_user_id: int) -> tuple[bool, str]:
    """Приостанавливает/возобновляет только участие заказа в общей рассылке."""
    global ads_data, orders_data
    order_id, ad_key, order, ad_data = resolve_ad_for_order_reference(order_ref)
    if not ad_key or not isinstance(ad_data, dict):
        return False, "Заказ не найден среди активных рекламных сообщений."

    status = str(ad_data.get("status") or "")
    if status not in {"approved", "active"}:
        return False, "Этот заказ сейчас не является активным."

    expires_at = parse_dt(ad_data.get("expires_at"))
    if expires_at and expires_at <= now_msk():
        return False, "Срок действия этого заказа уже истёк."

    timestamp = now_msk_str()
    current_value = bool(ad_data.get("broadcast_paused"))
    if current_value == paused:
        code = str(ad_data.get("order_code") or get_order_display_code(order_id or order_ref, order)).upper()
        return True, (
            f"Заказ {code} уже приостановлен." if paused
            else f"Заказ {code} уже участвует в рассылке."
        )

    canonical_ad_key = str(ad_data.get("ad_key") or ad_key)
    canonical_order_id = str(ad_data.get("order_id") or order_id or "")

    # Обновляем каноническую запись и её зеркала (active_ad, users, ключ пользователя).
    for key, candidate in ads_data.items():
        if not isinstance(candidate, dict):
            continue
        same_ad = (
            key == ad_key
            or str(candidate.get("ad_key") or "") == canonical_ad_key
            or (
                canonical_order_id
                and str(candidate.get("order_id") or "") == canonical_order_id
                and str(candidate.get("text") or "") == str(ad_data.get("text") or "")
            )
        )
        if not same_ad:
            continue
        candidate["broadcast_paused"] = paused
        if paused:
            candidate["broadcast_paused_at"] = timestamp
            candidate["broadcast_paused_by"] = admin_user_id
            candidate.pop("broadcast_resumed_at", None)
        else:
            candidate["broadcast_resumed_at"] = timestamp
            candidate["broadcast_resumed_by"] = admin_user_id
            candidate.pop("broadcast_paused_at", None)
            candidate.pop("broadcast_paused_by", None)

    related_order_ids = {value for value in (order_id, canonical_order_id) if value}
    for related_id in related_order_ids:
        related_order = orders_data.get(related_id)
        if not isinstance(related_order, dict):
            continue
        related_order["broadcast_paused"] = paused
        related_order["updated_at"] = timestamp
        related_order.setdefault("events", []).append(
            {
                "at": timestamp,
                "event": "broadcast_paused" if paused else "broadcast_resumed",
                "by": admin_user_id,
            }
        )

    save_ads()
    save_orders()
    code = str(ad_data.get("order_code") or get_order_display_code(order_id or order_ref, order)).upper()
    if paused:
        return True, f"Заказ {code} остановлен и больше не будет попадать в общую рассылку."
    return True, f"Заказ {code} снова участвует в общей рассылке."


def send_single_order_to_chat(chat_id: int, ad_data: dict[str, Any], attachment: str | None) -> str | None:
    result = send_message(
        chat_id,
        str(ad_data.get("text") or ""),
        attachment=attachment,
        keyboard=build_ad_openlink_keyboard(ad_data),
        rate_limit=False,
        api_client=get_bulk_vk(),
    )
    if result == "access_error":
        logger.warning("Разовая отправка заказа: потерян доступ к чату %s", chat_id)
        return "access_error"
    return None


def broadcast_single_order(order_ref: str, notify_chat_id: int | None = None) -> None:
    """Один раз отправляет только выбранный заказ во все рассылочные чаты."""
    global ads_data
    order_id, ad_key, order, ad_data = resolve_ad_for_order_reference(order_ref)
    if not ad_key or not isinstance(ad_data, dict):
        if notify_chat_id:
            send_message(notify_chat_id, "Заказ не найден среди активных рекламных сообщений.")
        return

    if str(ad_data.get("status") or "") not in {"approved", "active"}:
        if notify_chat_id:
            send_message(notify_chat_id, "Этот заказ сейчас не является активным.")
        return

    expires_at = parse_dt(ad_data.get("expires_at"))
    if expires_at and expires_at <= now_msk():
        if notify_chat_id:
            send_message(notify_chat_id, "Срок действия этого заказа уже истёк.")
        return

    safety = detect_military_recruitment(ad_data)
    if safety.get("blocked"):
        delete_ad_for_safety(ad_key, ad_data, safety, notify=True)
        if notify_chat_id:
            send_message(notify_chat_id, "Заказ не отправлен: объявление заблокировано проверкой безопасности.")
        return

    attachment, changed = resolve_ad_photo_attachment(ad_data, str(ad_data.get("ad_key") or ad_key))
    if changed:
        ads_data[ad_key] = ad_data
        save_ads()

    telegram_event_id = None
    with _chat_ids_lock:
        target_chats = [
            target_chat_id
            for target_chat_id in list(chat_ids)
            if admin_chat is not None
            and target_chat_id != admin_chat
            and len(str(target_chat_id)) == 10
            and str(target_chat_id).startswith("2")
        ]
    code = str(ad_data.get("order_code") or get_order_display_code(order_id or order_ref, order)).upper()
    if not target_chats:
        if notify_chat_id:
            if telegram_event_id:
                send_message(notify_chat_id, f"Заказ {code}: список VK-чатов пуст, но сообщение поставлено в очередь Telegram.")
            else:
                send_message(notify_chat_id, f"Заказ {code} не отправлен: список чатов пуст.")
        return

    sent_count = 0
    failed_count = 0
    logger.info("Разовая отправка заказа %s. Целевых чатов: %s", code, len(target_chats))
    try:
        with ThreadPoolExecutor(max_workers=min(BROADCAST_CHAT_WORKERS, len(target_chats)), thread_name_prefix="vk-order") as executor:
            futures = {
                executor.submit(send_single_order_to_chat, target_chat_id, dict(ad_data), attachment): target_chat_id
                for target_chat_id in target_chats
            }
            for future in as_completed(futures):
                target_chat_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    failed_count += 1
                    logger.exception("Ошибка разовой отправки заказа %s в чат %s: %s", code, target_chat_id, exc)
                    continue
                if result == "access_error":
                    failed_count += 1
                else:
                    sent_count += 1
    except Exception as exc:
        logger.exception("Критическая ошибка разовой отправки заказа %s: %s", code, exc)
        if notify_chat_id:
            send_message(notify_chat_id, f"Ошибка разовой отправки заказа {code}: {exc}")
        return

    if notify_chat_id:
        if failed_count:
            send_message(
                notify_chat_id,
                f"Разовая отправка заказа {code} завершена.\nУспешно: {sent_count}\nОшибок: {failed_count}",
            )
        else:
            send_message(notify_chat_id, f"Заказ {code} один раз отправлен во все чаты.\nОбработано чатов: {sent_count}")


def start_single_order_broadcast(order_ref: str, notify_chat_id: int | None = None) -> bool:
    global broadcast_thread
    if is_broadcast_quiet_time():
        allowed_at = next_broadcast_allowed_at()
        if notify_chat_id:
            send_message(
                notify_chat_id,
                "С 23:00 до 07:00 по МСК рассылка отключена.\n"
                f"Следующая возможная отправка: {allowed_at.strftime('%d.%m.%Y в %H:%M')} МСК.",
            )
        return False

    with broadcast_lock:
        if broadcast_thread and broadcast_thread.is_alive():
            if notify_chat_id:
                send_message(notify_chat_id, "Сейчас уже выполняется другая рассылка. Дождитесь её завершения.")
            return False
        broadcast_thread = threading.Thread(
            target=broadcast_single_order,
            kwargs={"order_ref": order_ref, "notify_chat_id": notify_chat_id},
            daemon=True,
        )
        broadcast_thread.start()
        return True

def sync_order_codes() -> None:
    global orders_data, ads_data
    changed_orders = False
    changed_ads = False

    for order_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        code = str(order.get("order_code", "") or "").upper()
        if not code or code.startswith("ORDER_") or code.isdigit():
            order["order_code"] = order_service.next_order_code(orders_data)
            changed_orders = True

    for key, ad_data in ads_data.items():
        if not isinstance(ad_data, dict):
            continue
        if key in {"users", "active_ad"}:
            continue
        if is_user_mirror_ad_key(key, ad_data):
            continue
        related_order_id = ad_data.get("order_id")
        if related_order_id and related_order_id in orders_data:
            desired = orders_data[related_order_id].get("order_code")
            if desired and ad_data.get("order_code") != desired:
                ad_data["order_code"] = desired
                changed_ads = True
            if ad_data.get("type") != orders_data[related_order_id].get("type"):
                ad_data["type"] = orders_data[related_order_id].get("type")
                changed_ads = True
            if ad_data.get("ad_key") != ("main_text_sale" if key == "main_text_sale" else key):
                ad_data["ad_key"] = "main_text_sale" if key == "main_text_sale" else key
                changed_ads = True
        elif key in orders_data:
            ad_data["order_id"] = key
            desired = orders_data[key].get("order_code")
            if desired and ad_data.get("order_code") != desired:
                ad_data["order_code"] = desired
                changed_ads = True
            if ad_data.get("type") != orders_data[key].get("type"):
                ad_data["type"] = orders_data[key].get("type")
                changed_ads = True
            if ad_data.get("ad_key") != ("main_text_sale" if key == "main_text_sale" else key):
                ad_data["ad_key"] = "main_text_sale" if key == "main_text_sale" else key
                changed_ads = True
        else:
            inferred_order_id = find_best_order_for_ad(ad_data)
            if inferred_order_id:
                ad_data["order_id"] = inferred_order_id
                ad_data["order_code"] = orders_data[inferred_order_id].get("order_code")
                ad_data["type"] = orders_data[inferred_order_id].get("type")
                ad_data["ad_key"] = "main_text_sale" if key == "main_text_sale" else key
                changed_ads = True
                continue
            code = str(ad_data.get("order_code", "") or "").upper()
            if not code or code.startswith("ORDER_") or code.isdigit():
                ad_data["order_code"] = order_service.next_order_code(orders_data)
                changed_ads = True

    if changed_orders:
        save_orders()
    if changed_ads:
        save_ads()


def _mark_order_safety_blocked(order_id: str, order: dict[str, Any], safety: dict[str, Any]) -> None:
    timestamp = now_msk_str()
    previous_status = order.get("status")
    blocked_status = "rejected" if previous_status == "pending" else "deleted"
    order["status"] = blocked_status
    order["updated_at"] = timestamp
    order["moderation_reason"] = safety.get("reason") or "Объявление заблокировано правилами безопасности рекламы."
    order["moderation_policy"] = safety.get("policy")
    order["moderation_legal_basis"] = safety.get("legal_basis")
    if blocked_status == "rejected":
        order["rejected_at"] = timestamp
    else:
        order["deleted_at"] = timestamp
    order.setdefault("events", []).append(
        {
            "at": timestamp,
            "event": "safety_blocked",
            "from_status": previous_status,
            "reason": order["moderation_reason"],
        }
    )


def _send_safety_notice_to_user(user_id: int | None, order_code: str | None, safety: dict[str, Any]) -> None:
    if not user_id:
        return
    safe_send_pm(
        int(user_id),
        render_military_recruitment_block_notice(order_code=order_code, reason=safety.get("reason")),
    )


def _ad_record_matches_target(
    key: str,
    ad_data: dict[str, Any],
    *,
    target_key: str,
    target_order_id: str | None,
    target_ad_key: str | None,
) -> bool:
    if key == target_key:
        return True
    if target_order_id and str(ad_data.get("order_id") or "") == target_order_id:
        return True
    if target_ad_key and (str(ad_data.get("ad_key") or "") == target_ad_key or key == target_ad_key):
        return True
    return False


def _remove_safety_blocked_ad_records(ad_key: str, ad_data: dict[str, Any]) -> bool:
    target_order_id = str(ad_data.get("order_id") or "") or (ad_key if ad_key in orders_data else None)
    target_ad_key = str(ad_data.get("ad_key") or "") or ad_key
    removed = False

    for key, value in list(ads_data.items()):
        if key == "users":
            if isinstance(value, dict):
                for user_key, user_ad in list(value.items()):
                    if isinstance(user_ad, dict) and _ad_record_matches_target(
                        user_key,
                        user_ad,
                        target_key=ad_key,
                        target_order_id=target_order_id,
                        target_ad_key=target_ad_key,
                    ):
                        value.pop(user_key, None)
                        removed = True
            continue
        if not isinstance(value, dict):
            continue
        if _ad_record_matches_target(
            key,
            value,
            target_key=ad_key,
            target_order_id=target_order_id,
            target_ad_key=target_ad_key,
        ):
            ads_data.pop(key, None)
            removed = True
    return removed


def reject_order_for_safety(
    order_id: str,
    order: dict[str, Any],
    safety: dict[str, Any],
    *,
    remove_ads: bool = True,
    notify: bool = True,
) -> None:
    _mark_order_safety_blocked(order_id, order, safety)
    if remove_ads:
        item_key = (
            "main_text_sale"
            if order.get("type") == "main_text"
            else str(order.get("ad_key") or order.get("source_order_id") or order.get("item_key") or order_id)
        )
        ad_data = ads_data.get(item_key)
        if isinstance(ad_data, dict):
            _remove_safety_blocked_ad_records(item_key, ad_data)
        elif item_key == "main_text_sale":
            ads_data.pop("main_text_sale", None)
    if notify:
        order["safety_notice_sent_at"] = now_msk_str()
        _send_safety_notice_to_user(
            int(order.get("user_id") or 0),
            get_order_display_code(order_id, order),
            safety,
        )
    save_orders()
    save_ads()


def delete_ad_for_safety(ad_key: str, ad_data: dict[str, Any], safety: dict[str, Any], *, notify: bool = True) -> bool:
    related_order_id = str(ad_data.get("order_id") or "") or (ad_key if ad_key in orders_data else "")
    if not related_order_id:
        related_order_id = find_best_order_for_ad(ad_data) or ""
    related_order = orders_data.get(related_order_id) if related_order_id else None

    if isinstance(related_order, dict):
        _mark_order_safety_blocked(related_order_id, related_order, safety)
        order_code = get_order_display_code(related_order_id, related_order)
        user_id = int(related_order.get("user_id") or 0)
    else:
        order_code = str(ad_data.get("order_code") or ad_data.get("ad_key") or ad_key).upper()
        user_id = int(ad_data.get("user_id") or 0)

    removed = _remove_safety_blocked_ad_records(ad_key, ad_data)
    if notify:
        if isinstance(related_order, dict):
            related_order["safety_notice_sent_at"] = now_msk_str()
        _send_safety_notice_to_user(user_id, order_code, safety)
    save_orders()
    save_ads()
    return removed


def process_safety_moderation() -> tuple[int, int]:
    global ads_data, orders_data
    orders_data = order_service.load_orders()
    ads_data = order_service.load_ads()
    blocked_orders = 0
    deleted_ads = 0

    for order_id, order in list(orders_data.items()):
        if not isinstance(order, dict):
            continue
        if order.get("status") not in {"pending", "approved", "active"}:
            continue
        safety = detect_military_recruitment(order)
        if not safety.get("blocked"):
            continue
        reject_order_for_safety(order_id, order, safety, remove_ads=True, notify=True)
        blocked_orders += 1

    for ad_key, ad_data in list(ads_data.items()):
        if ad_key in {"users", "active_ad"}:
            continue
        if not isinstance(ad_data, dict):
            continue
        if ad_key != "main_text_sale" and is_user_mirror_ad_key(ad_key, ad_data):
            continue
        if ad_data.get("status") not in {"approved", "active"}:
            continue
        safety = detect_military_recruitment(ad_data)
        if not safety.get("blocked"):
            continue
        if delete_ad_for_safety(ad_key, ad_data, safety, notify=True):
            deleted_ads += 1

    if blocked_orders or deleted_ads:
        logger.warning(f"Модерация безопасности: заблокировано заказов {blocked_orders}, удалено объявлений {deleted_ads}")
    return blocked_orders, deleted_ads



def _strict_test_vk_attachment(peer_id: int, attachment: str) -> tuple[bool, str]:
    """Проверяет не только успешность messages.send, но и наличие вложения
    в реально созданном сообщении VK.
    """
    ready = str(attachment or "").strip()
    if not ready:
        return False, "пустое вложение"

    expected_type = "video" if ready.startswith("video") else (
        "photo" if ready.startswith("photo") else None
    )

    client = get_bulk_vk()
    message_id = None
    try:
        message_id = client.messages.send(
            peer_id=int(peer_id),
            message="🎬 Проверка основного медиа",
            attachment=ready,
            random_id=generate_random_id(),
        )

        # VK иногда возвращает message_id, но фактически присылает сообщение
        # без проблемного video attachment. Поэтому читаем сообщение обратно.
        time.sleep(0.7)
        response = client.messages.getById(message_ids=str(message_id))
        items = response.get("items") or []
        if not items:
            return False, "VK отправил сообщение, но не удалось прочитать его обратно"

        sent = items[0] if isinstance(items[0], dict) else {}
        sent_attachments = sent.get("attachments") or []

        if expected_type:
            has_expected = any(
                isinstance(item, dict) and str(item.get("type") or "") == expected_type
                for item in sent_attachments
            )
            if not has_expected:
                return False, f"VK создал сообщение, но вложение {expected_type} из него исчезло"

        if not sent_attachments:
            return False, "VK создал сообщение без вложения"

        return True, ""

    except Exception as exc:
        return False, str(exc)

    finally:
        if message_id:
            try:
                client.messages.delete(
                    message_ids=[int(message_id)],
                    delete_for_all=1,
                )
            except Exception:
                pass


def _wait_until_vk_attachment_ready(
    peer_id: int,
    attachment: str,
    *,
    attempts: int = 12,
    pause: float = 1.5,
) -> tuple[bool, str]:
    """После загрузки видео VK может несколько секунд его обрабатывать."""
    last_error = ""
    for _ in range(max(1, attempts)):
        ok, error = _strict_test_vk_attachment(peer_id, attachment)
        if ok:
            return True, ""
        last_error = error
        time.sleep(max(0.2, pause))
    return False, last_error or "видео не стало доступным для отправки"



def _main_video_forward_state() -> dict[str, Any] | None:
    value = runtime_data.get("main_video_forward")
    normalized = _normalize_main_video_forward_data(value)
    if normalized is None:
        return None

    if normalized != value:
        runtime_data["main_video_forward"] = normalized
        try:
            save_runtime_data()
        except Exception:
            logger.exception("Не удалось мигрировать main_video_forward")

    return normalized


def _clear_main_video_forward_state() -> None:
    runtime_data.pop("main_video_forward", None)


def _build_source_forward(message: dict[str, Any]) -> dict[str, Any] | None:
    """Строит корректный messages.send.forward для исходного сообщения.

    Для сообщения из беседы используем ТОЛЬКО:
        peer_id + cmids

    Старое поле conversation_message_ids в актуальном messages.send.forward
    на практике может давать API error 100.
    """
    try:
        peer_id = int(message.get("peer_id") or 0)
    except (TypeError, ValueError):
        peer_id = 0

    try:
        cmid = int(message.get("conversation_message_id") or 0)
    except (TypeError, ValueError):
        cmid = 0

    try:
        message_id = int(message.get("id") or 0)
    except (TypeError, ValueError):
        message_id = 0

    if peer_id and cmid:
        return {
            "peer_id": peer_id,
            "cmids": [cmid],
            "is_reply": False,
        }

    # Fallback для сообщений, у которых доступен только глобальный message_id.
    if message_id:
        return {
            "message_ids": [message_id],
            "is_reply": False,
        }

    return None


def _normalize_main_video_forward_data(value: Any) -> dict[str, Any] | None:
    """Мигрирует старые сохранённые forward-данные в актуальную форму."""
    if not isinstance(value, dict):
        return None

    try:
        peer_id = int(value.get("peer_id") or 0)
    except (TypeError, ValueError):
        peer_id = 0

    raw_cmids = value.get("cmids")
    if not raw_cmids:
        # Миграция версии, где мы сохраняли conversation_message_ids.
        raw_cmids = value.get("conversation_message_ids")

    cmids: list[int] = []
    if isinstance(raw_cmids, (list, tuple)):
        for item in raw_cmids:
            try:
                ready = int(item)
            except (TypeError, ValueError):
                continue
            if ready > 0:
                cmids.append(ready)

    if peer_id and cmids:
        return {
            "peer_id": peer_id,
            "cmids": cmids[:100],
            "is_reply": bool(value.get("is_reply", False)),
        }

    raw_message_ids = value.get("message_ids")
    message_ids: list[int] = []
    if isinstance(raw_message_ids, (list, tuple)):
        for item in raw_message_ids:
            try:
                ready = int(item)
            except (TypeError, ValueError):
                continue
            if ready > 0:
                message_ids.append(ready)

    if message_ids:
        return {
            "message_ids": message_ids[:100],
            "is_reply": bool(value.get("is_reply", False)),
        }

    return None


def _forward_contains_video(item: dict[str, Any]) -> bool:
    """Проверяет видео и на верхнем уровне, и внутри пересланных сообщений."""
    attachments = item.get("attachments") or []
    for attachment in attachments:
        if isinstance(attachment, dict) and attachment.get("type") == "video":
            return True

    for forwarded in item.get("fwd_messages") or []:
        if isinstance(forwarded, dict) and _forward_contains_video(forwarded):
            return True

    reply = item.get("reply_message")
    if isinstance(reply, dict) and _forward_contains_video(reply):
        return True

    return False



def _vk_send_with_forward(
    client: Any,
    *,
    target_peer_id: int,
    message: str,
    forward_data: dict[str, Any],
    random_id: int,
    keyboard: str | None = None,
) -> Any:
    ready_forward = _normalize_main_video_forward_data(forward_data)
    if not ready_forward:
        raise ValueError("Некорректные данные исходного сообщения для пересылки")

    params: dict[str, Any] = {
        "peer_id": int(target_peer_id),
        "message": str(message or " "),
        "forward": json.dumps(
            ready_forward,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "random_id": int(random_id),
    }
    if keyboard:
        params["keyboard"] = keyboard

    return client.messages.send(**params)


def _strict_test_vk_forward(peer_id: int, forward_data: dict[str, Any]) -> tuple[bool, str]:
    """Проверяет, что group token реально может переслать исходное видео."""
    client = get_bulk_vk()
    message_id = None

    try:
        message_id = _vk_send_with_forward(
            client,
            target_peer_id=int(peer_id),
            message="🎬 Проверка видео",
            forward_data=forward_data,
            random_id=generate_random_id(),
        )

        time.sleep(0.7)
        response = client.messages.getById(message_ids=str(message_id))
        items = response.get("items") or []
        if not items:
            return False, "VK отправил сообщение, но не удалось прочитать его обратно"

        sent = items[0] if isinstance(items[0], dict) else {}
        if not _forward_contains_video(sent):
            return False, "пересланное сообщение пришло без видео"

        return True, ""

    except Exception as exc:
        return False, str(exc)

    finally:
        if message_id:
            try:
                client.messages.delete(
                    message_ids=[int(message_id)],
                    delete_for_all=1,
                )
            except Exception:
                pass


def _send_main_video_forwarded(
    chat_id: int,
    text_value: str,
    forward_data: dict[str, Any],
    keyboard: str | None = None,
    *,
    api_client: Any | None = None,
) -> Any:
    """Отправляет основной текст + кнопку + исходное видео через forward.

    Никакой пользовательский токен не используется.
    """
    client = api_client or get_bulk_vk()
    try:
        return _vk_send_with_forward(
            client,
            target_peer_id=int(chat_id),
            message=str(text_value or " "),
            forward_data=forward_data,
            random_id=generate_random_id(),
            keyboard=keyboard,
        )
    except Exception as exc:
        logger.error(
            "Не удалось переслать основное видео в чат %s: %s",
            chat_id,
            exc,
        )
        return None



def _download_url_to_file(
    url: str,
    target: Path,
    *,
    timeout: int = 180,
    max_bytes: int = 500 * 1024 * 1024,
) -> tuple[bool, str | None]:
    """Скачивает файл атомарно в локальную память бота."""
    ready_url = str(url or "").strip()
    if not ready_url:
        return False, "пустой URL"

    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".download")

    try:
        req = Request(
            ready_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Referer": "https://vk.com/",
            },
        )
        total = 0
        with urlopen(req, timeout=timeout) as response, temp_target.open("wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise RuntimeError("файл слишком большой для локального кэша")
                output.write(chunk)

        if not temp_target.exists() or temp_target.stat().st_size <= 0:
            raise RuntimeError("получен пустой файл")

        temp_target.replace(target)
        return True, None

    except Exception as exc:
        try:
            temp_target.unlink(missing_ok=True)
        except Exception:
            pass
        return False, str(exc)


def _extract_mp4_urls_from_player_html(player_html: str) -> list[str]:
    """Пытается найти прямые mp4 URL в HTML/JSON VK player."""
    raw = str(player_html or "")
    if not raw:
        return []

    # Разворачиваем типичные JSON escape-последовательности.
    raw = (
        raw.replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\u003d", "=")
        .replace("&amp;", "&")
    )

    found: list[tuple[int, str]] = []

    # Поля url720 / url1080 / mp4_720 и т.п.
    patterns = (
        re.compile(
            r'["\'](?:url|mp4_)(2160|1440|1080|720|480|360|240)["\']\s*:\s*["\']([^"\']+\.mp4[^"\']*)',
            re.IGNORECASE,
        ),
        re.compile(
            r'(https?://[^"\'<>\s]+\.mp4(?:\?[^"\'<>\s]*)?)',
            re.IGNORECASE,
        ),
    )

    for pattern in patterns:
        for match in pattern.finditer(raw):
            if match.lastindex == 2:
                try:
                    quality = int(match.group(1))
                except Exception:
                    quality = 0
                url = match.group(2)
            else:
                quality = 0
                url = match.group(1)

            url = html.unescape(str(url or "")).replace("\\/", "/").strip()
            if url.startswith("http"):
                found.append((quality, url))

    # Лучшее качество первым, потом без дублей.
    result: list[str] = []
    seen: set[str] = set()
    for _, url in sorted(found, key=lambda item: item[0], reverse=True):
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def _video_player_url(video: dict[str, Any]) -> str:
    for key in ("player", "player_url"):
        value = str(video.get(key) or "").strip()
        if value:
            return value
    return ""


def _discover_vk_video_download_url(video: dict[str, Any]) -> tuple[str | None, str | None]:
    """Пытается получить прямой mp4 БЕЗ user token.

    1. files из входящего attachment;
    2. group-token video.get;
    3. HTML публичного player, если VK отдал player URL.
    """
    full_video = _load_vk_video_object(video)

    direct = _best_vk_video_file_url(full_video)
    if direct:
        return direct, None

    player_url = _video_player_url(full_video) or _video_player_url(video)
    if not player_url:
        return None, "VK не отдал ни files, ни player URL"

    try:
        req = Request(
            player_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Referer": "https://vk.com/",
            },
        )
        with urlopen(req, timeout=30) as response:
            player_html = response.read(8_000_000).decode("utf-8", errors="replace")
    except Exception as exc:
        return None, f"не удалось открыть VK player: {exc}"

    candidates = _extract_mp4_urls_from_player_html(player_html)
    if not candidates:
        return None, "в VK player не найден прямой mp4"

    return candidates[0], None


def _save_main_video_local(video: dict[str, Any]) -> tuple[Path | None, str | None]:
    """Сохраняет видео физически на диск, если VK позволяет скачать исходник."""
    source_url, source_error = _discover_vk_video_download_url(video)
    if not source_url:
        return None, source_error

    ok, error = _download_url_to_file(
        source_url,
        MAIN_VIDEO_LOCAL_PATH,
        timeout=180,
        max_bytes=500 * 1024 * 1024,
    )
    if not ok:
        return None, error

    runtime_data[MAIN_VIDEO_LOCAL_KEY] = str(
        MAIN_VIDEO_LOCAL_PATH.relative_to(BASE_DIR)
    )
    return MAIN_VIDEO_LOCAL_PATH, None


def _saved_doc_attachment(saved: Any) -> str | None:
    """Нормализует результат docs.save из разных версий vk_api."""
    candidate = saved

    if isinstance(candidate, list) and candidate:
        candidate = candidate[0]

    if isinstance(candidate, dict) and isinstance(candidate.get("doc"), dict):
        candidate = candidate["doc"]

    if not isinstance(candidate, dict):
        return None

    owner_id = candidate.get("owner_id")
    doc_id = candidate.get("id")
    if owner_id is None or doc_id is None:
        return None

    ref = f"doc{owner_id}_{doc_id}"
    access_key = str(candidate.get("access_key") or "").strip()
    if access_key:
        ref += f"_{access_key}"
    return ref


def _upload_local_main_video_as_document(peer_id: int | None = None) -> tuple[str | None, str | None]:
    """Загружает локальный MP4 в сообщения как документ через GROUP token.

    В отличие от video.save, docs.getMessagesUploadServer доступен боту.
    Это даёт независимую от исходного сообщения резервную копию файла.
    """
    if not MAIN_VIDEO_LOCAL_PATH.exists() or MAIN_VIDEO_LOCAL_PATH.stat().st_size <= 0:
        return None, "локальный main_video.mp4 отсутствует"

    cached = str(runtime_data.get(MAIN_VIDEO_DOC_CACHE_KEY) or "").strip()
    if cached.startswith("doc"):
        return cached, None

    try:
        saved = media_vk_upload.document_message(
            str(MAIN_VIDEO_LOCAL_PATH),
            title="main_video.mp4",
            peer_id=int(peer_id or admin_chat),
        )
        ref = _saved_doc_attachment(saved)
        if not ref:
            return None, f"VK не вернул doc attachment: {saved}"

        runtime_data[MAIN_VIDEO_DOC_CACHE_KEY] = ref
        save_runtime_data()
        return ref, None

    except Exception as exc:
        return None, str(exc)


def _clear_local_main_video_cache() -> None:
    runtime_data.pop(MAIN_VIDEO_DOC_CACHE_KEY, None)
    runtime_data.pop(MAIN_VIDEO_LOCAL_KEY, None)
    try:
        MAIN_VIDEO_LOCAL_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def _clear_local_main_photo_cache() -> None:
    try:
        MAIN_PHOTO_LOCAL_PATH.unlink(missing_ok=True)
    except Exception:
        pass



MAIN_VIDEO_SOURCE_KEY = "main_video_source_message"

# Если рядом с main.py лежит main.mp4, он имеет приоритет как основное видео.
MAIN_MP4_PATH = BASE_DIR / "main.mp4"
MAIN_MP4_CACHE_KEY = "main_mp4_doc_attachment"
MAIN_MP4_MTIME_KEY = "main_mp4_mtime"


def _clear_main_video_source_state() -> None:
    runtime_data.pop(MAIN_VIDEO_SOURCE_KEY, None)


def _resolve_source_message_state(message: dict[str, Any]) -> dict[str, int] | None:
    """Получает peer_id/cmid/global message_id исходного сообщения с видео.

    Для group token методы чтения сообщений вызываются с group_id.
    """
    try:
        peer_id = int(message.get("peer_id") or 0)
    except (TypeError, ValueError):
        peer_id = 0

    try:
        cmid = int(message.get("conversation_message_id") or 0)
    except (TypeError, ValueError):
        cmid = 0

    try:
        message_id = int(message.get("id") or 0)
    except (TypeError, ValueError):
        message_id = 0

    if peer_id > 0 and cmid > 0:
        try:
            response = vk.messages.getByConversationMessageId(
                peer_id=peer_id,
                conversation_message_ids=str(cmid),
                group_id=int(group_id),
            )
            items = response.get("items") or []
            if items and isinstance(items[0], dict):
                resolved = int(items[0].get("id") or 0)
                if resolved > 0:
                    message_id = resolved
        except Exception as exc:
            logger.warning(
                "Не удалось дочитать исходное сообщение видео peer=%s cmid=%s: %s",
                peer_id,
                cmid,
                exc,
            )

    result: dict[str, int] = {}
    if peer_id > 0:
        result["peer_id"] = peer_id
    if cmid > 0:
        result["cmid"] = cmid
    if message_id > 0:
        result["message_id"] = message_id

    if ("peer_id" in result and "cmid" in result) or "message_id" in result:
        return result
    return None


def _get_main_video_source_state() -> dict[str, int] | None:
    """Возвращает источник видео.

    ВАЖНО: global message_id не обязателен.
    Для пересылки достаточно peer_id + conversation_message_id (cmid).
    """
    raw = runtime_data.get(MAIN_VIDEO_SOURCE_KEY)
    if not isinstance(raw, dict):
        return None

    result: dict[str, int] = {}

    for key in ("message_id", "peer_id", "cmid"):
        try:
            value = int(raw.get(key) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            result[key] = value

    # Валидно либо peer_id + cmid, либо global message_id.
    if (
        ("peer_id" in result and "cmid" in result)
        or "message_id" in result
    ):
        return result

    return None


def _message_has_video(message_obj: dict[str, Any]) -> bool:
    for item in message_obj.get("attachments") or []:
        if isinstance(item, dict) and item.get("type") == "video":
            return True

    for fwd in message_obj.get("fwd_messages") or []:
        if isinstance(fwd, dict) and _message_has_video(fwd):
            return True

    reply = message_obj.get("reply_message")
    if isinstance(reply, dict) and _message_has_video(reply):
        return True

    return False


def _send_and_verify_direct_video(
    peer_id: int,
    text_value: str,
    attachment_ref: str,
    keyboard: str | None = None,
    *,
    api_client: Any | None = None,
    delete_after_check: bool = False,
) -> tuple[Any, bool, str]:
    """Проверяет именно то, что VK не просто принял messages.send,
    а реально оставил video attachment в отправленном сообщении.
    """
    client = api_client or get_bulk_vk()
    params: dict[str, Any] = {
        "peer_id": int(peer_id),
        "message": str(text_value or " "),
        "attachment": str(attachment_ref),
        "random_id": generate_random_id(),
    }
    if keyboard:
        params["keyboard"] = keyboard

    sent_id = None
    try:
        sent_id = client.messages.send(**params)
    except Exception as exc:
        return None, False, str(exc)

    try:
        time.sleep(0.45)
        response = client.messages.getById(message_ids=str(sent_id), group_id=int(group_id))
        items = response.get("items") or []
        if items and isinstance(items[0], dict) and _message_has_video(items[0]):
            return sent_id, True, ""
        return sent_id, False, "VK создал сообщение, но удалил video attachment"
    except Exception as exc:
        # Если проверка недоступна, не считаем это подтверждённым direct-режимом.
        return sent_id, False, f"не удалось подтвердить video attachment: {exc}"
    finally:
        if delete_after_check and sent_id:
            try:
                client.messages.delete(
                    message_ids=str(sent_id),
                    delete_for_all=1,
                )
            except Exception:
                pass


def _send_source_video_forward(
    chat_id: int,
    text_value: str,
    source_state: dict[str, int],
    keyboard: str | None = None,
    *,
    api_client: Any | None = None,
) -> Any:
    """Надёжный fallback без user token.

    Используем старый верхнеуровневый параметр forward_messages с глобальным
    message_id. Он не требует video.save и не требует прямой mp4-ссылки.
    """
    client = api_client or get_bulk_vk()
    source_message_id = int(source_state.get("message_id") or 0)
    if source_message_id <= 0:
        return None

    params: dict[str, Any] = {
        "peer_id": int(chat_id),
        "message": str(text_value or " "),
        "forward_messages": str(source_message_id),
        "random_id": generate_random_id(),
    }
    if keyboard:
        params["keyboard"] = keyboard

    try:
        return client.messages.send(**params)
    except Exception as exc:
        logger.error(
            "Не удалось перенести исходное видео message_id=%s в peer=%s: %s",
            source_message_id,
            chat_id,
            exc,
        )
        return None



def _local_main_mp4_exists() -> bool:
    try:
        return MAIN_MP4_PATH.exists() and MAIN_MP4_PATH.is_file() and MAIN_MP4_PATH.stat().st_size > 0
    except Exception:
        return False


def _main_mp4_mtime() -> float:
    try:
        return float(MAIN_MP4_PATH.stat().st_mtime)
    except Exception:
        return 0.0


def _clear_main_mp4_cache() -> None:
    runtime_data.pop(MAIN_MP4_CACHE_KEY, None)
    runtime_data.pop(MAIN_MP4_MTIME_KEY, None)


def _upload_main_mp4_as_message_doc(peer_id: int | None = None) -> tuple[str | None, str | None]:
    """Берёт ./main.mp4 и загружает его в VK как файл сообщения.

    Используется только group token. Если файл не менялся, повторно не грузим.
    """
    if not _local_main_mp4_exists():
        _clear_main_mp4_cache()
        return None, "Файл main.mp4 рядом с main.py не найден"

    current_mtime = _main_mp4_mtime()
    cached_ref = str(runtime_data.get(MAIN_MP4_CACHE_KEY) or "").strip()
    try:
        cached_mtime = float(runtime_data.get(MAIN_MP4_MTIME_KEY) or 0.0)
    except (TypeError, ValueError):
        cached_mtime = 0.0

    if cached_ref.startswith("doc") and abs(cached_mtime - current_mtime) < 0.001:
        return cached_ref, None

    try:
        saved = media_vk_upload.document_message(
            str(MAIN_MP4_PATH),
            title="main.mp4",
            peer_id=int(peer_id or admin_chat),
        )
    except Exception as exc:
        return None, f"Не удалось загрузить main.mp4 в VK: {exc}"

    ref = _saved_doc_attachment(saved)
    if not ref:
        return None, f"VK не вернул attachment после загрузки main.mp4: {saved}"

    runtime_data[MAIN_MP4_CACHE_KEY] = ref
    runtime_data[MAIN_MP4_MTIME_KEY] = current_mtime
    try:
        save_runtime_data()
    except Exception:
        logger.exception("Не удалось сохранить кэш main.mp4")

    return ref, None


def _resolve_main_message_media_attachment() -> tuple[str | None, str | None]:
    attachment = upload_message_photo()
    return attachment, None if attachment else "Основное медиа не сохранено"


def _video_ref_from_object(video: dict[str, Any]) -> str:
    owner_id = video.get("owner_id")
    video_id = video.get("id")
    if owner_id is None or video_id is None:
        return ""

    ref = f"video{owner_id}_{video_id}"
    access_key = str(video.get("access_key") or "").strip()
    if access_key:
        ref += f"_{access_key}"
    return ref


def _best_vk_video_file_url(video: dict[str, Any]) -> str:
    files = video.get("files")
    if not isinstance(files, dict):
        return ""

    # Берём лучшее обычное mp4, которое VK дал через API.
    for key in (
        "mp4_2160",
        "mp4_1440",
        "mp4_1080",
        "mp4_720",
        "mp4_480",
        "mp4_360",
        "mp4_240",
    ):
        value = str(files.get(key) or "").strip()
        if value:
            return value

    return ""


def _load_vk_video_object(video: dict[str, Any]) -> dict[str, Any]:
    """Пытается получить полные данные видео, включая files."""
    if _best_vk_video_file_url(video):
        return dict(video)

    owner_id = video.get("owner_id")
    video_id = video.get("id")
    if owner_id is None or video_id is None:
        return dict(video)

    access_key = str(video.get("access_key") or "").strip()
    videos_value = f"{owner_id}_{video_id}"
    if access_key:
        videos_value += f"_{access_key}"

    try:
        response = media_vk.video.get(videos=videos_value, count=1)
        items = response.get("items") or []
        if items and isinstance(items[0], dict):
            merged = dict(video)
            merged.update(items[0])
            return merged
    except Exception as exc:
        logger.warning("Не удалось получить полные данные VK-видео %s: %s", videos_value, exc)

    return dict(video)


def _reupload_main_video_to_community(video: dict[str, Any]) -> tuple[str | None, str | None]:
    """Скачивает исходный mp4 и загружает копию в видео сообщества.

    Это нужно для роликов, которые видны админу в сообщении, но сообщество
    не имеет права пересылать по исходному videoOWNER_ID_ID.
    """
    full_video = _load_vk_video_object(video)
    source_url = _best_vk_video_file_url(full_video)
    if not source_url:
        return None, "VK не предоставил прямой mp4-файл этого видео"

    videos_dir = BASE_DIR / "videos"
    videos_dir.mkdir(parents=True, exist_ok=True)
    target = videos_dir / "main_video.mp4"
    temp_target = videos_dir / "main_video.download"

    try:
        req = Request(
            source_url,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urlopen(req, timeout=120) as response, temp_target.open("wb") as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)

        if not temp_target.exists() or temp_target.stat().st_size <= 0:
            raise RuntimeError("получен пустой видеофайл")

        temp_target.replace(target)
    except Exception as exc:
        try:
            temp_target.unlink(missing_ok=True)
        except Exception:
            pass
        return None, f"не удалось скачать видео: {exc}"

    title = " ".join(str(full_video.get("title") or "Основное видео рассылки").split()).strip()
    if not title:
        title = "Основное видео рассылки"

    try:
        uploaded = media_vk_upload.video(
            video_file=str(target),
            name=title[:128],
            description="Видео основного сообщения бота",
            group_id=int(group_id),
            no_comments=True,
        )
    except Exception as exc:
        return None, f"не удалось загрузить копию видео в сообщество: {exc}"

    if not isinstance(uploaded, dict):
        return None, "VK вернул неизвестный ответ после загрузки видео"

    uploaded_owner_id = uploaded.get("owner_id")
    uploaded_video_id = uploaded.get("video_id", uploaded.get("id"))
    if uploaded_owner_id is None or uploaded_video_id is None:
        return None, f"VK не вернул owner_id/video_id: {uploaded}"

    ref = f"video{uploaded_owner_id}_{uploaded_video_id}"
    access_key = str(uploaded.get("access_key") or "").strip()
    if access_key:
        ref += f"_{access_key}"

    return ref, None


def _extract_vk_video_ref_from_text(text_value: str) -> tuple[str | None, int | None]:
    """Понимает ссылки вида vk.com/video-123_456 и video-123_456."""
    raw = str(text_value or "")
    match = re.search(
        r'(?:https?://(?:www\.)?vk\.com/)?video(-?\d+)_(\d+)(?:[^\s]*?access_key=([A-Za-z0-9_-]+))?',
        raw,
        re.IGNORECASE,
    )
    if not match:
        return None, None

    owner_id = int(match.group(1))
    video_id = int(match.group(2))
    access_key = str(match.group(3) or "").strip()

    ref = f"video{owner_id}_{video_id}"
    if access_key:
        ref += f"_{access_key}"
    return ref, owner_id


def _probe_plain_vk_video(attachment_ref: str) -> tuple[bool, str]:
    """Проверяет в админ-чате, что вложение реально отображается как VIDEO."""
    probe_peer = int(admin_chat or 0)
    if probe_peer <= 0:
        return False, "admin_chat не настроен"

    client = get_bulk_vk()
    sent_id = None
    try:
        sent_id = client.messages.send(
            peer_id=probe_peer,
            message="🎬",
            attachment=attachment_ref,
            random_id=generate_random_id(),
        )

        # Проверяем историю чата: нужен именно attachment type=video.
        time.sleep(0.5)
        response = client.messages.getHistory(
            peer_id=probe_peer,
            count=8,
            group_id=int(group_id),
        )
        for item in response.get("items") or []:
            if not isinstance(item, dict):
                continue
            if int(item.get("out") or 0) != 1:
                continue
            if str(item.get("text") or "").strip() != "🎬":
                continue
            if _message_has_video(item):
                return True, ""
            return False, "VK отправил сообщение, но убрал video-вложение"

        return False, "не удалось найти проверочное сообщение"

    except Exception as exc:
        return False, str(exc)

    finally:
        if sent_id:
            try:
                client.messages.delete(
                    message_ids=str(sent_id),
                    delete_for_all=1,
                )
            except Exception:
                pass


def save_main_photo_from_message(message: dict[str, Any]) -> tuple[bool, str]:
    """Старое рабочее поведение: сохраняем готовый VK attachment как есть."""
    global message_photo_path, message_photo_attachment
    global uploaded_message_photo, uploaded_message_photo_at

    attachments = message.get("attachments") or []
    if not attachments:
        return False, "Прикрепите фото или видео к сообщению с командой .редоснфото"

    attachment_ref = extract_attachment_reference(message)

    if attachment_ref and attachment_ref.startswith(("photo", "video")):
        message_photo_path = attachment_ref
        message_photo_attachment = attachment_ref
        uploaded_message_photo = attachment_ref
        uploaded_message_photo_at = time.time()

        # Старые экспериментальные режимы больше не вмешиваются.
        runtime_data.pop(MAIN_VIDEO_SOURCE_KEY, None)
        runtime_data.pop("main_video_forward", None)
        runtime_data.pop("main_video_mode", None)
        runtime_data.pop(MAIN_VIDEO_DOC_CACHE_KEY, None)
        runtime_data.pop(MAIN_VIDEO_LOCAL_KEY, None)
        runtime_data.pop(MAIN_MP4_CACHE_KEY, None)
        runtime_data.pop(MAIN_MP4_MTIME_KEY, None)

        save_runtime_data()
        logger.info("Основное VK-вложение сохранено напрямую: %s", attachment_ref)
        return True, "✅ Основное вложение бота успешно обновлено."

    # Fallback старого кода для фото, если attachment reference почему-то не собрался.
    photo_attachment = None
    for item in attachments:
        if isinstance(item, dict) and item.get("type") == "photo":
            photo_attachment = item.get("photo")
            break

    if not isinstance(photo_attachment, dict):
        return False, "Поддерживаются фото и видео."

    sizes = photo_attachment.get("sizes") or []
    if not sizes:
        return False, "Не удалось получить размеры фото."

    best = max(sizes, key=lambda x: x.get("width", 0) * x.get("height", 0))
    url = best.get("url")
    if not url:
        return False, "Не удалось получить ссылку на фото."

    photos_dir = BASE_DIR / "photos"
    photos_dir.mkdir(parents=True, exist_ok=True)
    target = photos_dir / "main_photo.jpg"

    try:
        with urlopen(url) as response:
            target.write_bytes(response.read())
    except Exception as exc:
        return False, f"Не удалось сохранить фото: {exc}"

    message_photo_path = "photos/main_photo.jpg"
    message_photo_attachment = None
    uploaded_message_photo = None
    uploaded_message_photo_at = 0.0
    save_runtime_data()

    return True, "✅ Основное вложение бота успешно обновлено."


def _telegram_media_files(values: Any) -> list[str]:
    if isinstance(values, list):
        raw_values = values
    elif values:
        raw_values = [values]
    else:
        raw_values = []
    result: list[str] = []
    for value in raw_values:
        ready = str(value or "").strip()
        if ready and ready not in result:
            result.append(ready)
    return result


def build_telegram_main_broadcast_item() -> dict[str, Any] | None:
    if not str(message_text or "").strip():
        return None
    photo_ref = upload_message_photo() or message_photo_attachment or ""
    photo_files: list[str] = []
    raw_path = str(message_photo_path or "").strip()
    if raw_path and not raw_path.startswith(("photo", "video", "doc")):
        photo_files.append(raw_path)
    return {
        "text": str(message_text or ""),
        "photo_refs": str(photo_ref or ""),
        "photo_files": photo_files,
        "button_title": str(main_button_title or ""),
        "button_link": str(main_button_link or ""),
        "delay_after": float(interval_sec or 0),
    }


def build_telegram_ad_broadcast_item(ad_data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(ad_data, dict):
        return None
    text = str(ad_data.get("text") or "")
    photo_refs = str(ad_data.get("photo") or "")
    photo_files = _telegram_media_files(ad_data.get("photo_files"))
    if not text.strip() and not photo_refs.strip() and not photo_files:
        return None
    return {
        "text": text,
        "photo_refs": photo_refs,
        "photo_files": photo_files,
        "button_title": str(ad_data.get("button_title") or ""),
        "button_link": str(ad_data.get("button_link") or ""),
        "delay_after": float(interval_sec or 0),
    }


def build_telegram_full_broadcast_items() -> list[dict[str, Any]]:
    global ads_data
    ads_data = order_service.load_ads()
    items: list[dict[str, Any]] = []

    for ad_key, ad_data in get_active_random_orders():
        safety = detect_military_recruitment(ad_data)
        if safety.get("blocked"):
            logger.warning("Telegram-синхронизация: заказ %s пропущен проверкой безопасности", ad_key)
            continue
        item = build_telegram_ad_broadcast_item(ad_data)
        if item:
            items.append(item)

    jobs_item = build_telegram_jobs_broadcast_item()
    if jobs_item:
        items.append(jobs_item)

    main_item = build_telegram_main_broadcast_item()
    if main_item:
        items.append(main_item)

    main_text_sale = ads_data.get("main_text_sale", {})
    if isinstance(main_text_sale, dict) and main_text_sale.get("status") == "active" and not main_text_sale.get("broadcast_paused"):
        expires_at = parse_dt(main_text_sale.get("expires_at"))
        if not expires_at or expires_at > now_msk():
            safety = detect_military_recruitment(main_text_sale)
            if not safety.get("blocked"):
                item = build_telegram_ad_broadcast_item(main_text_sale)
                if item:
                    items.append(item)
    return items


def queue_telegram_broadcast(source: str, items: list[dict[str, Any]]) -> str | None:
    """Telegram полностью отключён. Функция оставлена только для совместимости старых вызовов."""
    return None


def broadcast_message(notify_chat_id: int | None = None) -> None:
    try:
        process_safety_moderation()
    except Exception as exc:
        logger.exception("Ошибка проверки безопасности перед рассылкой: %s", exc)

    vk_items = build_vk_full_broadcast_items()
    telegram_event_id = None
    with _chat_ids_lock:
        target_chats = [
            chat_id for chat_id in list(chat_ids)
            if admin_chat is not None and chat_id != admin_chat and len(str(chat_id)) == 10 and str(chat_id).startswith("2")
        ]
    logger.info(
        "Рассылка запущена. Целевых VK-чатов: %s; фоновых потоков: %s",
        len(target_chats),
        min(BROADCAST_CHAT_WORKERS, len(target_chats)) if target_chats else 0,
    )
    if not target_chats:
        if notify_chat_id:
            send_message(notify_chat_id, "⚠️ Рассылка не запущена: список VK-чатов пуст.")
        return
    if not vk_items:
        if notify_chat_id:
            send_message(
                notify_chat_id,
                "⚠️ В VK-рассылке нет активных сообщений.",
                rate_limit=False,
            )
        return
    stats_run_id = _stats_begin_run(
        source="full",
        target_chats=target_chats,
        items=vk_items,
    )

    sent_count = 0
    failed_count = 0
    try:
        with ThreadPoolExecutor(
            max_workers=min(BROADCAST_CHAT_WORKERS, len(target_chats)),
            thread_name_prefix="vk-broadcast",
        ) as executor:
            futures = {
                executor.submit(send_broadcast_to_chat, chat_id, vk_items, stats_run_id): chat_id
                for chat_id in target_chats
            }
            for future in as_completed(futures):
                chat_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    failed_count += 1
                    logger.exception(f"Ошибка рассылки в чат {chat_id}: {exc}")
                    continue
                if result == "access_error":
                    failed_count += 1
                else:
                    sent_count += 1
        _stats_finish_run(
            stats_run_id,
            sent_chats=sent_count,
            failed_chats=failed_count,
        )
        # Первое обновление делаем сразу; дальше фон обновляет каждые 2 минуты.
        try:
            refresh_broadcast_read_stats(run_id=stats_run_id)
        except Exception as stats_exc:
            logger.debug("Первое обновление read-статистики не удалось: %s", stats_exc)

        if failed_count:
            logger.warning(f"Рассылка завершена с ошибками. Обработано: {sent_count}, ошибок: {failed_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"⚠️ Рассылка завершена с ошибками.\nУспешно: {sent_count}\nОшибок доступа/отправки: {failed_count}")
        else:
            logger.info(f"Рассылка завершена. Обработано чатов: {sent_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"✅ Рассылка завершена.\nVK-чатов: {sent_count}")
    except Exception as exc:
        try:
            _stats_finish_run(
                stats_run_id,
                sent_chats=sent_count,
                failed_chats=max(1, failed_count),
            )
        except Exception:
            pass
        logger.exception(f"Критическая ошибка рассылки: {exc}")
        if notify_chat_id:
            send_message(notify_chat_id, f"❌ Ошибка рассылки: {exc}")


def start_broadcast(notify_chat_id: int | None = None) -> bool:
    global broadcast_thread
    if is_broadcast_quiet_time():
        allowed_at = next_broadcast_allowed_at()
        logger.info(
            "Рассылка отложена: действует ночной запрет до %s МСК.",
            allowed_at.strftime("%d.%m.%Y %H:%M"),
        )
        if notify_chat_id:
            send_message(
                notify_chat_id,
                "🌙 С 23:00 до 07:00 по МСК рассылка отключена.\n"
                f"Следующая возможная отправка: {allowed_at.strftime('%d.%m.%Y в %H:%M')} МСК.",
            )
        return False

    with broadcast_lock:
        if broadcast_thread and broadcast_thread.is_alive():
            logger.info("Попытка повторно запустить уже активную рассылку.")
            return False
        logger.info("Создаю поток рассылки.")
        broadcast_thread = threading.Thread(
            target=broadcast_message,
            kwargs={"notify_chat_id": notify_chat_id},
            daemon=True,
        )
        broadcast_thread.start()
        return True


def build_vk_gzov_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    jobs_block = get_jobs_broadcast_message()
    if jobs_block.get("enabled") and str(jobs_block.get("text") or "").strip():
        attachment, changed = resolve_jobs_broadcast_attachment(jobs_block)
        if changed:
            with _runtime_data_lock:
                runtime_data["jobs_broadcast_message"] = jobs_block
                save_runtime_data()
        items.append({
            "text": str(jobs_block.get("text") or ""),
            "attachment": attachment,
            "keyboard": build_jobs_broadcast_keyboard(),
            "delay_after": float(interval_sec or 0),
        })
    if message_text:
        items.append({
            "text": str(message_text or ""),
            "attachment": upload_message_photo(),
            "keyboard": build_buy_ad_keyboard(),
            "delay_after": float(interval_sec or 0),
        })
    return items


def send_gzov_to_chat(chat_id: int, items: list[dict[str, Any]]) -> Any:
    logger.info(f"GZOV: отправка основных сообщений в чат {chat_id}")
    return send_broadcast_to_chat(chat_id, items)


def broadcast_gzov(notify_chat_id: int | None = None) -> None:
    telegram_event_id = None
    vk_gzov_items = build_vk_gzov_items()
    with _chat_ids_lock:
        target_chats = [
            chat_id for chat_id in list(chat_ids)
            if admin_chat is not None and chat_id != admin_chat and len(str(chat_id)) == 10 and str(chat_id).startswith("2")
        ]
    logger.info(
        "GZOV запущен. Целевых VK-чатов: %s; фоновых потоков: %s",
        len(target_chats),
        min(BROADCAST_CHAT_WORKERS, len(target_chats)) if target_chats else 0,
    )
    if not target_chats:
        if notify_chat_id:
            send_message(notify_chat_id, "⚠️ GZOV не запущен: список VK-чатов пуст.")
        return
    sent_count = 0
    failed_count = 0
    try:
        with ThreadPoolExecutor(
            max_workers=min(BROADCAST_CHAT_WORKERS, len(target_chats)),
            thread_name_prefix="vk-gzov",
        ) as executor:
            futures = {executor.submit(send_gzov_to_chat, chat_id, vk_gzov_items): chat_id for chat_id in target_chats}
            for future in as_completed(futures):
                chat_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    failed_count += 1
                    logger.exception(f"Ошибка GZOV в чат {chat_id}: {exc}")
                    continue
                if result == "access_error":
                    failed_count += 1
                else:
                    sent_count += 1
        if failed_count:
            logger.warning(f"GZOV завершён с ошибками. Обработано: {sent_count}, ошибок: {failed_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"⚠️ GZOV завершён с ошибками.\nУспешно: {sent_count}\nОшибок доступа/отправки: {failed_count}")
        else:
            logger.info(f"GZOV завершён. Обработано чатов: {sent_count}")
            if notify_chat_id:
                send_message(notify_chat_id, f"✅ GZOV завершён.\nVK-чатов: {sent_count}")
    except Exception as exc:
        logger.exception(f"Критическая ошибка GZOV: {exc}")
        if notify_chat_id:
            send_message(notify_chat_id, f"❌ Ошибка GZOV: {exc}")


def start_gzov(notify_chat_id: int | None = None) -> bool:
    global gzov_thread
    with gzov_lock:
        if gzov_thread and gzov_thread.is_alive():
            logger.info("Попытка повторно запустить уже активный GZOV.")
            return False
        gzov_thread = threading.Thread(
            target=broadcast_gzov,
            kwargs={"notify_chat_id": notify_chat_id},
            name="vk-gzov-supervisor",
            daemon=True,
        )
        gzov_thread.start()
        return True


def auto_broadcast_loop() -> None:
    global last_broadcast_time, cd_min, pending_broadcast_after_quiet
    consecutive_errors = 0
    was_quiet = is_broadcast_quiet_time()

    while True:
        try:
            current_cd = max(float(cd_min), 0.0)
            timer_expired = time.time() - last_broadcast_time >= current_cd * 60
            quiet_now = is_broadcast_quiet_time()

            if quiet_now:
                if timer_expired and not pending_broadcast_after_quiet:
                    pending_broadcast_after_quiet = True
                    logger.info(
                        "Таймер рассылки истёк ночью. Отправка поставлена в очередь на 07:00 МСК."
                    )
            else:
                quiet_just_ended = was_quiet and not quiet_now
                if pending_broadcast_after_quiet or timer_expired:
                    if quiet_just_ended and pending_broadcast_after_quiet:
                        logger.info("Ночной запрет завершён. Запускаю отложенную рассылку в 07:00 МСК.")
                    if start_broadcast():
                        last_broadcast_time = time.time()
                        pending_broadcast_after_quiet = False

            was_quiet = quiet_now
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            logger.error(f"Ошибка авторассылки по таймеру ({consecutive_errors}): {exc}")
            if consecutive_errors > 10:
                logger.critical("Слишком много ошибок авторассылки, сбрасываю таймер")
                last_broadcast_time = time.time()
                pending_broadcast_after_quiet = False
                consecutive_errors = 0
        time.sleep(5)


def handle_group_info(chat_id: int) -> None:
    send_message(
        chat_id,
        "ℹ️ ИНФОРМАЦИЯ О НАС\n\nМы занимаемся продвижением каналов.\n📊 150+ чатов для рассылки\n\nНажмите кнопку ниже, чтобы перейти к покупке рекламы.",
        keyboard=build_buy_ad_keyboard(),
    )


def resolve_user_id(token: str) -> int | None:
    token = token.strip()
    mention_match = re.match(r"\[id(\d+)\|", token)
    if mention_match:
        return int(mention_match.group(1))
    direct_id = re.match(r"id(\d+)$", token, re.IGNORECASE)
    if direct_id:
        return int(direct_id.group(1))
    if token.startswith("@"):
        token = token[1:]
    try:
        user = vk.users.get(user_ids=token)
        if user:
            return int(user[0]["id"])
    except Exception as exc:
        logger.warning(f"Не удалось разрешить пользователя {token}: {exc}")
    return None


def extract_target_user(message: dict[str, Any], text: str) -> int | None:
    reply_message = message.get("reply_message")
    if isinstance(reply_message, dict) and reply_message.get("from_id"):
        return int(reply_message["from_id"])

    for raw_part in (text or "").replace("\n", " ").split():
        part = raw_part.strip(",")
        mention_match = re.match(r"\[id(\d+)\|", part)
        if mention_match:
            return int(mention_match.group(1))
        if part.startswith("@"):
            resolved = resolve_user_id(part[1:])
            if resolved:
                return resolved
        direct_id = re.match(r"id(\d+)$", part, re.IGNORECASE)
        if direct_id:
            return int(direct_id.group(1))
        if "vk.com/" in part or "vk.ru/" in part:
            tail = part.rstrip("/").split("/")[-1]
            resolved = resolve_user_id(tail)
            if resolved:
                return resolved
    return None


def create_direct_order(
    user_id: int,
    days: int,
    text_value: str,
    photo: str | None,
    photo_files: list[str | None] | None,
    admin_user_id: int,
) -> tuple[str, dict[str, Any]]:
    global ads_data
    order_id = order_service.next_order_id(orders_data)
    order_no = order_service.next_order_number(orders_data)
    order_code = order_service.next_order_code(orders_data)
    now = now_msk()
    expires_at = (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    order = {
        "order_no": order_no,
        "order_code": order_code,
        "type": "new_ad",
        "status": "approved",
        "user_id": user_id,
        "text": text_value,
        "photo": photo,
        "photo_files": photo_files or [],
        "price": 0,
        "days": days,
        "rate_name": f"{days} дн.",
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "approved_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "approved_by": admin_user_id,
        "events": [
            {"at": now.strftime("%Y-%m-%d %H:%M:%S"), "event": "created_by_admin"},
            {"at": now.strftime("%Y-%m-%d %H:%M:%S"), "event": "approved"},
        ],
    }
    orders_data[order_id] = order
    ad_data = {
        "ad_key": order_id,
        "order_id": order_id,
        "order_code": order_code,
        "type": "new_ad",
        "user_id": user_id,
        "text": text_value,
        "photo": photo,
        "photo_files": photo_files or [],
        "price": 0,
        "days": days,
        "rate_name": f"{days} дн.",
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at": expires_at,
        "status": "approved",
    }
    ads_data[order_id] = ad_data
    save_orders()
    save_ads()
    return order_id, ad_data


def build_delete_order_keyboard(target_user_id: int, requester_id: int) -> str | None:
    keyboard = VkKeyboard(inline=True)
    has_items = False

    user_ads = order_service.get_user_active_ads(ads_data, target_user_id)
    for ad_data in user_ads:
        item_key = ad_data.get("ad_key") or next((key for key, value in ads_data.items() if value is ad_data), "extra_active")
        preview = (ad_data.get("text") or "Активная рассылка").strip()
        label = preview[:30] + ("..." if len(preview) > 30 else "")
        keyboard.add_callback_button(
            f"🗑 Рассылка: {label}",
            color=VkKeyboardColor.NEGATIVE,
            payload={"command": "delete_user_order", "target_user_id": target_user_id, "requester_id": requester_id, "item_key": item_key},
        )
        keyboard.add_line()
        has_items = True

    main_sale = ads_data.get("main_text_sale")
    if isinstance(main_sale, dict) and main_sale.get("user_id") == target_user_id:
        preview = (main_sale.get("text") or "Основная реклама").strip()
        label = preview[:30] + ("..." if len(preview) > 30 else "")
        keyboard.add_callback_button(
            f"🗑 Осн. реклама: {label}",
            color=VkKeyboardColor.NEGATIVE,
            payload={"command": "delete_user_order", "target_user_id": target_user_id, "requester_id": requester_id, "item_key": "main_text_sale"},
        )
        has_items = True

    return keyboard.get_keyboard() if has_items else None


def build_single_delete_confirm(target_user_id: int, requester_id: int, ads_data_ref: dict, svc) -> str:
    keyboard = VkKeyboard(inline=True)
    user_ads = svc.get_user_active_ads(ads_data_ref, target_user_id)
    main_sale = ads_data_ref.get("main_text_sale")
    has_main = isinstance(main_sale, dict) and main_sale.get("user_id") == target_user_id
    item_key = None
    if user_ads:
        item_key = user_ads[0].get("ad_key") or "extra_active"
    elif has_main:
        item_key = "main_text_sale"
    if item_key:
        keyboard.add_callback_button(
            "✅ Да, удалить",
            color=VkKeyboardColor.NEGATIVE,
            payload={"command": "delete_user_order", "target_user_id": target_user_id, "requester_id": requester_id, "item_key": item_key},
        )
        keyboard.add_line()
    keyboard.add_callback_button("❌ Нет, оставить", color=VkKeyboardColor.SECONDARY, payload={"command": "cancel_delete"})
    return keyboard.get_keyboard()


def build_single_order_keyboard(order_id: str, order: dict[str, Any], requester_id: int, has_check: bool = False, message_id: int | None = None) -> str:
    keyboard = VkKeyboard(inline=True)
    item_key = "main_text_sale" if order.get("type") == "main_text" else (order.get("ad_key") or order_id)
    keyboard.add_callback_button(
        "✏️ Редактировать текст",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_order_text", "order_id": order_id, "requester_id": requester_id},
    )
    keyboard.add_callback_button(
        "🖼 Удалить фото",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": "remove_order_photo", "order_id": order_id, "requester_id": requester_id},
    )
    keyboard.add_line()
    keyboard.add_callback_button(
        "📎 Добавить фото",
        color=VkKeyboardColor.SECONDARY,
        payload={"command": "add_order_photo", "order_id": order_id, "requester_id": requester_id},
    )
    keyboard.add_line()
    if order_has_button(order):
        keyboard.add_callback_button(
            "✏️ Название кнопки",
            color=VkKeyboardColor.PRIMARY,
            payload={"command": "edit_order_button_title", "order_id": order_id, "requester_id": requester_id},
        )
        keyboard.add_callback_button(
            "🔗 Ссылка кнопки",
            color=VkKeyboardColor.PRIMARY,
            payload={"command": "edit_order_button_link", "order_id": order_id, "requester_id": requester_id},
        )
        keyboard.add_line()
        keyboard.add_callback_button(
            "🗑 Удалить кнопку",
            color=VkKeyboardColor.NEGATIVE,
            payload={"command": "remove_order_button", "order_id": order_id, "requester_id": requester_id},
        )
    else:
        keyboard.add_callback_button(
            "➕ Добавить кнопку",
            color=VkKeyboardColor.POSITIVE,
            payload={"command": "add_order_button", "order_id": order_id, "requester_id": requester_id},
        )
    keyboard.add_line()
    keyboard.add_callback_button(
        "⏳ Изменить срок",
        color=VkKeyboardColor.PRIMARY,
        payload={"command": "edit_order_days", "order_id": order_id, "requester_id": requester_id},
    )
    if has_permission(requester_id, "dev"):
        keyboard.add_callback_button(
            "🗑 Удалить рекламу",
            color=VkKeyboardColor.NEGATIVE,
            payload={
                "command": "delete_user_order",
                "order_id": order_id,
                "target_user_id": int(order.get("user_id") or 0),
                "requester_id": requester_id,
                "item_key": item_key,
            },
        )
    if has_check:
        keyboard.add_line()
        keyboard.add_callback_button(
            "📎 Показать чек",
            color=VkKeyboardColor.SECONDARY,
            payload={"command": "show_check_order", "order_id": order_id, "requester_id": requester_id},
        )
    if message_id:
        keyboard.add_line()
        keyboard.add_callback_button(
            "❌ Выйти",
            color=VkKeyboardColor.SECONDARY,
            payload={"command": "exit_order_view", "message_id": message_id, "requester_id": requester_id},
        )
    return keyboard.get_keyboard()


def delete_direct_order(user_id: int, item_key: str | None = None) -> bool:
    global ads_data
    removed = False
    user_key = str(user_id)

    if item_key in {None, "extra_active"} or (item_key and item_key != "main_text_sale"):
        if item_key and item_key not in {None, "extra_active", "main_text_sale"}:
            ad_data = ads_data.get(item_key)
            if isinstance(ad_data, dict) and ad_data.get("user_id") == user_id:
                ads_data.pop(item_key, None)
                removed = True
                if isinstance(ads_data.get(user_key), dict) and ads_data[user_key].get("order_id") == ad_data.get("order_id"):
                    ads_data.pop(user_key, None)
                    removed = True
                users_ads = ads_data.get("users")
                if isinstance(users_ads, dict):
                    user_ad = users_ads.get(user_key)
                    if isinstance(user_ad, dict) and user_ad.get("order_id") == ad_data.get("order_id"):
                        users_ads.pop(user_key, None)
                        removed = True
                active_ad = ads_data.get("active_ad")
                if isinstance(active_ad, dict) and active_ad.get("order_id") == ad_data.get("order_id"):
                    ads_data.pop("active_ad", None)
                    removed = True
        else:
            if user_key in ads_data:
                ads_data.pop(user_key, None)
                removed = True
            users_ads = ads_data.get("users")
            if isinstance(users_ads, dict):
                if users_ads.pop(user_key, None) is not None:
                    removed = True
            active_ad = ads_data.get("active_ad")
            if isinstance(active_ad, dict) and active_ad.get("user_id") == user_id:
                ads_data.pop("active_ad", None)
                removed = True
        candidate_orders = [
            (order_id, order)
            for order_id, order in orders_data.items()
            if order.get("user_id") == user_id and order.get("status") == "approved" and order.get("type") in {"new_ad", "update_ad", "extend_ad", "renew_ad"}
        ]
        if item_key and item_key not in {None, "extra_active", "main_text_sale"}:
            candidate_orders = [(order_id, order) for order_id, order in candidate_orders if order_id == item_key]
        if candidate_orders:
            candidate_orders.sort(key=lambda pair: pair[1].get("approved_at") or pair[1].get("created_at") or "")
            order = candidate_orders[-1][1]
            order["status"] = "deleted"
            order["deleted_at"] = now_msk_str()
            order.setdefault("events", []).append({"at": order["deleted_at"], "event": "deleted"})
            removed = True

    if item_key in {None, "main_text_sale"}:
        main_sale = ads_data.get("main_text_sale")
        if isinstance(main_sale, dict) and main_sale.get("user_id") == user_id:
            ads_data.pop("main_text_sale", None)
            removed = True
        main_orders = [
            order
            for order in orders_data.values()
            if order.get("user_id") == user_id and order.get("status") == "approved" and order.get("type") == "main_text"
        ]
        if main_orders:
            main_orders.sort(key=lambda item: item.get("approved_at") or item.get("created_at") or "")
            order = main_orders[-1]
            order["status"] = "deleted"
            order["deleted_at"] = now_msk_str()
            order.setdefault("events", []).append({"at": order["deleted_at"], "event": "deleted"})
            removed = True

    if removed:
        save_orders()
        save_ads()
    return removed


def get_order_item_key(order_id: str, order: dict[str, Any] | None) -> str:
    if not order:
        return order_id
    if order.get("type") == "main_text":
        return "main_text_sale"
    return str(order.get("ad_key") or order_id)


def get_ads_record_for_order(order_id: str, order: dict[str, Any] | None) -> dict[str, Any] | None:
    if not order:
        return None
    item_key = get_order_item_key(order_id, order)
    ad_data = ads_data.get(item_key)
    return ad_data if isinstance(ad_data, dict) else None


def update_order_and_ad(order_id: str, updater) -> bool:
    order = orders_data.get(order_id)
    if not order:
        return False
    updater(order)
    order["updated_at"] = now_msk_str()
    order.setdefault("events", []).append({"at": order["updated_at"], "event": "updated"})

    ad_data = get_ads_record_for_order(order_id, order)
    if ad_data is not None:
        updater(ad_data)
        ad_data["updated_at"] = order["updated_at"]
    save_orders()
    save_ads()
    return True


def apply_order_text_change(order_id: str, text_value: str) -> bool:
    clean_text = text_value.strip()
    if not clean_text:
        return False
    return update_order_and_ad(order_id, lambda data: data.__setitem__("text", clean_text))


def apply_order_days_change(order_id: str, days: int) -> bool:
    if days <= 0:
        return False
    return update_order_and_ad(order_id, lambda data: data.__setitem__("days", days))


def order_has_button(order: dict[str, Any] | None) -> bool:
    if not isinstance(order, dict):
        return False
    return bool(str(order.get("button_title") or "").strip() and str(order.get("button_link") or "").strip())


def apply_order_button_title_change(order_id: str, title: str) -> bool:
    clean_title = title.strip()
    if not clean_title:
        return False
    return update_order_and_ad(order_id, lambda data: data.__setitem__("button_title", clean_title))


def apply_order_button_link_change(order_id: str, link: str) -> bool:
    clean_link = link.strip()
    if not re.match(r"^https?://", clean_link, flags=re.IGNORECASE):
        return False
    return update_order_and_ad(order_id, lambda data: data.__setitem__("button_link", clean_link))


def apply_order_button_add(order_id: str, title: str, link: str) -> bool:
    clean_title = title.strip()
    clean_link = link.strip()
    if not clean_title or not re.match(r"^https?://", clean_link, flags=re.IGNORECASE):
        return False

    def set_button(data: dict[str, Any]) -> None:
        data["button_title"] = clean_title
        data["button_link"] = clean_link

    return update_order_and_ad(order_id, set_button)


def apply_order_button_remove(order_id: str) -> bool:
    def clear_button(data: dict[str, Any]) -> None:
        data["button_title"] = None
        data["button_link"] = None
        data["custom_button_price"] = 0

    return update_order_and_ad(order_id, clear_button)


def apply_order_photo_remove(order_id: str) -> bool:
    order = orders_data.get(order_id)
    if not order:
        return False

    def clear_photo(data: dict[str, Any]) -> None:
        data["photo"] = None
        data["photo_files"] = []

    return update_order_and_ad(order_id, clear_photo)


def apply_order_photo_add(order_id: str, attachment_ref: str | list[str], photo_files: list[str | None] | None = None) -> bool:
    refs = [part.strip() for part in (attachment_ref if isinstance(attachment_ref, str) else ",".join(attachment_ref)).split(",") if part.strip()]
    if not refs:
        return False
    incoming_files = normalize_photo_files(photo_files)

    def merge_photo(data: dict[str, Any]) -> None:
        current_parts = [part.strip() for part in str(data.get("photo") or "").split(",") if part.strip()]
        current_files = normalize_photo_files(data.get("photo_files"))
        for index, ref in enumerate(refs):
            if ref not in current_parts:
                current_parts.append(ref)
                current_files.append(incoming_files[index] if index < len(incoming_files) else None)
        data["photo"] = ",".join(current_parts) if current_parts else None
        data["photo_files"] = current_files

    return update_order_and_ad(order_id, merge_photo)


def handle_admin_order_edit_input(chat_id: int, user_id: int, text: str, message: dict[str, Any] | None = None) -> bool:
    state = USER_STATES.get(str(user_id)) or {}
    pending = state.get("admin_order_edit")
    if not isinstance(pending, dict):
        return False
    if pending.get("chat_id") != chat_id:
        return False
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        state.pop("admin_order_edit", None)
        send_message(chat_id, "❌ Права администратора больше не подтверждены. Редактирование заказа отменено.")
        return True

    order_id = str(pending.get("order_id") or "")
    mode = pending.get("mode")
    attachments = (message or {}).get("attachments") or []
    if order_id not in orders_data:
        state.pop("admin_order_edit", None)
        send_message(chat_id, "❌ Заказ не найден.")
        return True

    if mode == "text":
        safety = detect_military_recruitment({"text": text})
        if safety.get("blocked"):
            state.pop("admin_order_edit", None)
            order = orders_data.get(order_id, {})
            notice = render_military_recruitment_block_notice(
                order_code=get_order_display_code(order_id, order),
                reason=safety.get("reason"),
            )
            send_message(chat_id, notice)
            if isinstance(order, dict) and order.get("user_id"):
                safe_send_pm(int(order.get("user_id")), notice)
            return True
        if not apply_order_text_change(order_id, text):
            send_message(chat_id, "❌ Отправьте новый текст для заказа обычным сообщением.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Текст заказа {get_order_display_code(order_id)} обновлён.")
        return True

    if mode == "days":
        try:
            days = int(text.strip())
        except ValueError:
            send_message(chat_id, "❌ Отправьте срок числом, например: 30")
            return True
        if not apply_order_days_change(order_id, days):
            send_message(chat_id, "❌ Срок должен быть положительным числом.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Срок заказа {get_order_display_code(order_id)} обновлён: {days} дн.")
        return True

    if mode == "photo":
        if not attachments:
            send_message(chat_id, "❌ Отправьте фото или видео следующим сообщением.")
            return True
        attachment_refs = [ref for ref in extract_attachment_references(message or {}) if ref.startswith(("photo", "video"))]
        photo_files = save_photo_files_from_attachments(
            BASE_DIR,
            attachments,
            prefix=f"admin_{order_id}_{int(time.time() * 1000)}",
            allowed_kinds={"photo", "video"},
            logger=logger,
        )
        if not apply_order_photo_add(order_id, attachment_refs, photo_files):
            send_message(chat_id, "❌ Не удалось добавить фото к заказу.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Фото добавлено к заказу {get_order_display_code(order_id)}.", attachment=",".join(attachment_refs))
        return True

    if mode == "button_add_title":
        title = text.strip()
        if not title:
            send_message(chat_id, "❌ Название кнопки не может быть пустым.")
            return True
        pending["button_title"] = title
        pending["mode"] = "button_add_link"
        send_message(chat_id, "🔗 Теперь отправьте ссылку кнопки, начиная с http:// или https://")
        return True

    if mode == "button_add_link":
        title = str(pending.get("button_title") or "").strip()
        if not apply_order_button_add(order_id, title, text):
            send_message(chat_id, "❌ Неверная ссылка. Отправьте ссылку, начинающуюся с http:// или https://")
            return True
        state.pop("admin_order_edit", None)
        send_message(
            chat_id,
            f"✅ Кнопка добавлена к заказу {get_order_display_code(order_id)}.\n"
            f"Название: {title}\n"
            f"Ссылка: {text.strip()}",
        )
        return True

    if mode == "button_title":
        if not apply_order_button_title_change(order_id, text):
            send_message(chat_id, "❌ Название кнопки не может быть пустым.")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Название кнопки заказа {get_order_display_code(order_id)} изменено на: {text.strip()}")
        return True

    if mode == "button_link":
        if not apply_order_button_link_change(order_id, text):
            send_message(chat_id, "❌ Неверная ссылка. Отправьте ссылку, начинающуюся с http:// или https://")
            return True
        state.pop("admin_order_edit", None)
        send_message(chat_id, f"✅ Ссылка кнопки заказа {get_order_display_code(order_id)} изменена на: {text.strip()}")
        return True

    state.pop("admin_order_edit", None)
    return False


def delete_chat_message(peer_id: int, message_id: int) -> None:
    client = vk if threading.get_ident() == _main_vk_thread_ident else get_bulk_vk()
    try:
        client.messages.delete(message_ids=message_id, delete_for_all=1)
    except Exception:
        try:
            client.messages.delete(peer_id=peer_id, cmids=message_id, delete_for_all=1)
        except Exception as exc:
            logger.warning(f"Не удалось удалить сообщение {message_id} в чате {peer_id}: {exc}")


def delete_chat_message_by_cmid(peer_id: int, cmid: int) -> None:
    client = vk if threading.get_ident() == _main_vk_thread_ident else get_bulk_vk()
    try:
        client.messages.delete(peer_id=peer_id, cmids=cmid, delete_for_all=1)
    except Exception as exc:
        logger.warning(f"Не удалось удалить сообщение с conversation_message_id={cmid} в чате {peer_id}: {exc}")


def handle_admin_text_command(chat_id: int, user_id: int, text: str, message: dict[str, Any]) -> bool:
    global ads_data
    normalized = str(text or "").strip().lower()
    is_admin_order_command = (
        normalized.startswith(".добзаказ")
        or normalized.startswith(".удзаказ")
        or normalized == ".заказ"
        or normalized.startswith(".заказ ")
        or normalized.startswith(".измзаказ")
    )
    if not is_admin_order_command:
        return False
    if chat_id != admin_chat:
        send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
        return True
    if not has_permission(user_id, "admin"):
        send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
        return True

    if text.startswith(".добзаказ"):
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Доступ запрещён. Только разработчик.")
            return True
        now_ts = time.time()
        cmd_key = f"dobzakaz:{user_id}:{text}"
        if cmd_key in _last_admin_commands and now_ts - _last_admin_commands[cmd_key] < _ADMIN_COMMAND_TTL:
            logger.info(f"Пропущен дубль команды .добзаказ от {user_id}")
            return True
        _last_admin_commands[cmd_key] = now_ts
        parts = text.split(" ", 3)
        if len(parts) < 4:
            send_message(chat_id, "Использование: .добзаказ @username [дни] [текст]")
            return True
        target_token = parts[1]
        try:
            days = int(parts[2])
        except ValueError:
            send_message(chat_id, "Дни должны быть числом.")
            return True
        text_value = parts[3].strip()
        if not text_value:
            send_message(chat_id, "Укажите текст заказа.")
            return True
        target_user_id = resolve_user_id(target_token)
        if not target_user_id:
            send_message(chat_id, "Не удалось определить пользователя.")
            return True
        safety = detect_military_recruitment({"text": text_value})
        if safety.get("blocked"):
            notice = render_military_recruitment_block_notice(reason=safety.get("reason"))
            send_message(chat_id, notice)
            safe_send_pm(target_user_id, notice)
            return True
        photo = extract_attachment_reference(message)
        photo_files = save_photo_files_from_attachments(
            BASE_DIR,
            message.get("attachments") or [],
            prefix=f"direct_{target_user_id}_{int(time.time() * 1000)}",
            allowed_kinds={"photo", "video"},
            logger=logger,
        )
        order_id, ad_data = create_direct_order(target_user_id, days, text_value, photo, photo_files, user_id)
        safe_send_pm(target_user_id, f"✅ Ваш заказ добавлен администратором.\nРассылка активирована до: {ad_data['expires_at']}")
        send_message(chat_id, f"✅ Заказ {get_order_display_code(order_id)} добавлен для id{target_user_id}.")
        return True

    if text.startswith(".удзаказ"):
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Доступ запрещён. Только разработчик.")
            return True
        parts = text.split(" ", 1)
        if len(parts) < 2:
            send_message(chat_id, "Использование: .удзаказ @username")
            return True
        target_user_id = resolve_user_id(parts[1].strip())
        if not target_user_id:
            send_message(chat_id, "Не удалось определить пользователя.")
            return True
        user_ads = order_service.get_user_active_ads(ads_data, target_user_id)
        main_sale = ads_data.get("main_text_sale")
        has_main = isinstance(main_sale, dict) and main_sale.get("user_id") == target_user_id
        total = len(user_ads) + (1 if has_main else 0)
        if total == 0:
            send_message(chat_id, "Активный заказ для этого пользователя не найден.")
            return True
        if total == 1:
            keyboard = build_single_delete_confirm(target_user_id, user_id, ads_data, order_service)
            send_message(chat_id, f"⚠️ У пользователя {render_user_mention(target_user_id)} 1 объявление. Удалить?", keyboard=keyboard)
            return True
        keyboard = build_delete_order_keyboard(target_user_id, user_id)
        if not keyboard:
            send_message(chat_id, "Активный заказ для этого пользователя не найден.")
            return True
        send_message(chat_id, f"Выберите рекламу пользователя {render_user_mention(target_user_id)}, которую нужно удалить:", keyboard=keyboard)
        return True

    if text == ".заказ" or text.startswith(".заказ "):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_message(chat_id, "❌ Формат: .заказ [order_code]")
            return True
        order_ref = parts[1].strip()
        resolved = resolve_order_reference(order_ref)
        if not resolved or resolved not in orders_data:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        order = orders_data[resolved]
        user_label = render_user_mention(int(order.get("user_id"))) if order.get("user_id") else "неизвестно"
        has_check = bool(order.get("attachment"))
        message_id = send_message(chat_id, render_order_details(resolved, order, user_label), attachment=primary_photo_reference_from_order(order))
        send_message(
            chat_id,
            "Действия по заказу:",
            keyboard=build_single_order_keyboard(
                resolved,
                order,
                user_id,
                has_check=has_check,
                message_id=message_id if isinstance(message_id, int) else None,
            ),
        )
        return True

    if text.startswith(".измзаказ"):
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Доступ запрещён. Только разработчик.")
            return True
        parts = text.split(maxsplit=3)
        if len(parts) < 4:
            send_message(chat_id, "❌ Формат: .измзаказ <order_id|номер> <поле> <значение>")
            return True
        order_ref = parts[1].strip()
        field = parts[2].strip()
        raw_value = parts[3].strip()
        resolved = None
        if order_ref in orders_data:
            resolved = order_ref
        else:
            for candidate_id, order in orders_data.items():
                if str(order.get("order_no")) == order_ref or str(order.get("order_code", "")).upper() == order_ref.upper():
                    resolved = candidate_id
                    break
        if not resolved or resolved not in orders_data:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        if field not in {"text", "price", "days", "rate_name", "status", "type", "photo"}:
            send_message(chat_id, "❌ Нельзя менять это поле.")
            return True
        value: Any = raw_value
        if field in {"price", "days"}:
            try:
                value = int(raw_value)
            except ValueError:
                send_message(chat_id, "❌ Для price/days нужно число.")
                return True
        if field == "text":
            safety = detect_military_recruitment({"text": str(value)})
            if safety.get("blocked"):
                send_message(chat_id, render_military_recruitment_block_notice(reason=safety.get("reason")))
                return True
        orders_data[resolved][field] = value
        orders_data[resolved]["updated_at"] = now_msk_str()
        save_orders()
        send_message(chat_id, f"✅ Заказ {resolved} обновлён: {field} = {value}")
        return True

    return False


def handle_admin_order_action(chat_id: int, user_id: int, payload: dict[str, Any]) -> bool:
    global ads_data
    command = payload.get("command")
    order_id = payload.get("order_id")
    if command not in {"view_order", "approve_order", "reject_order", "delete_user_order", "show_check_order", "support_open_chat", "support_close", "exit_order_view", "edit_order_text", "remove_order_photo", "add_order_photo", "edit_order_days", "add_order_button", "edit_order_button_title", "edit_order_button_link", "remove_order_button", "history_page", "cancel_delete"}:
        return False
    event_object = payload.get("_event_object")
    if chat_id != admin_chat or not has_permission(user_id, "admin"):
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Нет прав.")
        send_message(chat_id, "❌ Административные кнопки доступны только администрации в админ-чате.")
        return True
    if command == "cancel_delete":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Удаление отменено.")
        send_message(chat_id, "✅ Удаление отменено.")
        return True
    if command == "support_open_chat":
        target_user_id = int(payload.get("user_id") or 0)
        if not target_user_id:
            send_message(chat_id, "❌ Не удалось определить покупателя.")
            return True
        safe_send_pm(target_user_id, "✅ Администратор с вами! Объясните ситуацию.")
        USER_STATES.setdefault(str(target_user_id), {"menu_sent": True})["support_mode"] = True
        try:
            event_object = payload.get("_event_object")
            if isinstance(event_object, dict):
                answer_callback_event(
                    event_object,
                    link=f"https://vk.com/gim{group_id}?sel={target_user_id}",
                )
        except Exception as exc:
            logger.warning(f"Не удалось открыть чат поддержки: {exc}")
        send_message(chat_id, f"✅ Открываю чат с {render_user_mention(target_user_id)} и отправляю уведомление пользователю.")
        return True
    if command == "support_close":
        target_user_id = int(payload.get("user_id") or 0)
        if not target_user_id:
            send_message(chat_id, "❌ Не удалось определить покупателя.")
            return True
        state = USER_STATES.setdefault(str(target_user_id), {"menu_sent": True})
        state.pop("support_mode", None)
        state.pop("flow", None)
        safe_send_pm(target_user_id, "✅ Обращение закрыто. Бот снова доступен.")
        event_object = payload.get("_event_object")
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Обращение закрыто.")
        send_message(chat_id, f"✅ Вопрос пользователя {render_user_mention(target_user_id)} закрыт. Бот снова отвечает в ЛС.")
        return True
    if command == "delete_user_order":
        requester_id = int(payload.get("requester_id") or 0)
        target_user_id = int(payload.get("target_user_id") or 0)
        item_key = payload.get("item_key")
        if requester_id != user_id:
            send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал команду.")
            return True
        if not has_permission(user_id, "dev"):
            send_message(chat_id, "❌ Удаление рекламы доступно только разработчику.")
            return True
        if not target_user_id:
            send_message(chat_id, "❌ Не удалось определить пользователя для удаления.")
            return True
        if delete_direct_order(target_user_id, item_key):
            safe_send_pm(target_user_id, "❌ Ваш активный заказ был удалён администратором.")
            send_message(chat_id, f"✅ Выбранная реклама пользователя {render_user_mention(target_user_id)} удалена.")
        else:
            send_message(chat_id, "Активный заказ для этого пользователя не найден.")
        return True
    if command == "exit_order_view":
        requester_id = int(payload.get("requester_id") or 0)
        if requester_id and requester_id != user_id:
            send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал команду.")
            return True
        target_message_id = int(payload.get("message_id") or 0)
        if target_message_id:
            delete_chat_message(chat_id, target_message_id)
        event_object = payload.get("_event_object")
        if isinstance(event_object, dict):
            cmid = int(event_object.get("conversation_message_id") or 0)
            if cmid:
                delete_chat_message_by_cmid(chat_id, cmid)
        return True
    if command == "history_page":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Открываю историю...")
        page = int(payload.get("page") or 0)
        history_keyboard = build_orders_history_keyboard(orders_data, page=page)
        send_message(chat_id, render_orders_history_text(orders_data, page=page), keyboard=history_keyboard)
        return True
    order = orders_data.get(order_id)
    user_label = render_user_mention(int(order.get("user_id"))) if order and order.get("user_id") else "неизвестно"
    requester_id = int(payload.get("requester_id") or 0)
    if command in {"edit_order_text", "remove_order_photo", "add_order_photo", "edit_order_days", "add_order_button", "edit_order_button_title", "edit_order_button_link", "remove_order_button", "show_check_order"} and requester_id and requester_id != user_id:
        send_message(chat_id, "❌ Эти кнопки доступны только администратору, который вызвал команду.")
        return True
    if command == "view_order":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Открываю заявку...")
        readonly = bool(payload.get("readonly"))
        history_page = int(payload.get("history_page") or 0)
        send_message(
            chat_id,
            render_order_details(order_id, order, user_label),
            keyboard=build_order_action_keyboard(
                order_id,
                order.get("status") if order else None,
                has_check=bool(order and order.get("attachment")),
                has_button=order_has_button(order),
                readonly=readonly,
                history_page=history_page,
            ),
            attachment=primary_photo_reference_from_order(order),
        )
        return True
    if command == "edit_order_text":
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "text", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"✏️ Отправьте новый текст для заказа {get_order_display_code(order_id)} следующим сообщением.")
        return True
    if command == "remove_order_photo":
        if not order:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        if apply_order_photo_remove(order_id):
            send_message(chat_id, f"✅ Фото у заказа {get_order_display_code(order_id)} удалено.")
        else:
            send_message(chat_id, "❌ Не удалось удалить фото.")
        return True
    if command == "add_order_photo":
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "photo", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"📎 Отправьте новое фото или видео для заказа {get_order_display_code(order_id)} следующим сообщением.")
        return True
    if command == "add_order_button":
        if not order:
            send_message(chat_id, "❌ Заказ не найден.")
            return True
        if order_has_button(order):
            send_message(chat_id, "❌ У этого заказа уже есть кнопка. Используйте изменение названия или ссылки.")
            return True
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "button_add_title", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"➕ Отправьте название новой кнопки для заказа {get_order_display_code(order_id)}.")
        return True
    if command == "edit_order_button_title":
        if not order or not order_has_button(order):
            send_message(chat_id, "❌ У этого заказа нет кнопки.")
            return True
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "button_title", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"✏️ Отправьте новое название кнопки для заказа {get_order_display_code(order_id)}.")
        return True
    if command == "edit_order_button_link":
        if not order or not order_has_button(order):
            send_message(chat_id, "❌ У этого заказа нет кнопки.")
            return True
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "button_link", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"🔗 Отправьте новую ссылку кнопки для заказа {get_order_display_code(order_id)}. Ссылка должна начинаться с http:// или https://")
        return True
    if command == "remove_order_button":
        if not order or not order_has_button(order):
            send_message(chat_id, "❌ У этого заказа нет кнопки.")
            return True
        if apply_order_button_remove(order_id):
            send_message(chat_id, f"✅ Кнопка у заказа {get_order_display_code(order_id)} удалена.")
        else:
            send_message(chat_id, "❌ Не удалось удалить кнопку.")
        return True
    if command == "edit_order_days":
        USER_STATES.setdefault(str(user_id), {})["admin_order_edit"] = {"mode": "days", "order_id": order_id, "chat_id": chat_id}
        send_message(chat_id, f"⏳ Отправьте новый срок в днях для заказа {get_order_display_code(order_id)}.")
        return True
    if command == "show_check_order":
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Показываю чек...")
        attachment = check_attachment_reference_from_order(order)
        if not attachment and not check_file_link_from_order(order):
            send_message(chat_id, "❌ У этой заявки нет прикреплённого чека.")
            return True
        send_check_to_chat(chat_id, order_id, order)
        return True
    if command == "approve_order" and order:
        safety = detect_military_recruitment(order)
        if safety.get("blocked"):
            if isinstance(event_object, dict):
                answer_callback_event(event_object, text="Заявка заблокирована правилами.")
            reject_order_for_safety(order_id, order, safety, remove_ads=True, notify=True)
            send_message(chat_id, render_military_recruitment_block_notice(order_code=get_order_display_code(order_id, order), reason=safety.get("reason")))
            return True
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Заявка одобрена.")
        _, ads_data, _ = order_service.approve_order(orders_data, ads_data, order_id)
        save_orders()
        save_ads()
        append_income(order["user_id"], int(order.get("price", 0) or 0), f"{order.get('type')} - {order.get('rate_name')}")
        safe_send_pm(order["user_id"], "✅ Ваш заказ одобрен!\nРассылка активирована.")
        send_message(chat_id, render_order_result(order_id, order, user_label))
        return True
    if command == "reject_order" and order:
        if isinstance(event_object, dict):
            answer_callback_event(event_object, text="Заявка отклонена.")
        order_service.reject_order(orders_data, order_id)
        save_orders()
        safe_send_pm(order["user_id"], "❌ Ваша заявка отклонена.")
        send_message(chat_id, render_order_result(order_id, order, user_label))
        return True
    return True


def main() -> None:
    global ads_data, orders_data, admin_chat, message_text, cd_min, interval_sec, service_send_cooldown_sec, expiration_thread, auto_broadcast_thread, applications_cleanup_thread, last_broadcast_time, main_button_title, main_button_link, infozov_thread, pm_membership_watch_thread, broadcast_stats_thread, application_recovery_thread
    logger.info("Бот запущен pid=%s", os.getpid())
    last_broadcast_time = time.time()
    sync_order_codes()
    process_expiring_ads()
    process_safety_moderation()
    if expiration_thread is None or not expiration_thread.is_alive():
        expiration_thread = threading.Thread(target=expiration_monitor, daemon=True)
        expiration_thread.start()
    if auto_broadcast_thread is None or not auto_broadcast_thread.is_alive():
        auto_broadcast_thread = threading.Thread(target=auto_broadcast_loop, daemon=True)
        auto_broadcast_thread.start()
    if applications_cleanup_thread is None or not applications_cleanup_thread.is_alive():
        applications_cleanup_thread = threading.Thread(target=applications_cleanup_loop, daemon=True)
        applications_cleanup_thread.start()
    if pm_membership_watch_thread is None or not pm_membership_watch_thread.is_alive():
        pm_membership_watch_thread = threading.Thread(
            target=pm_membership_auto_watch_loop,
            name="pm-membership-auto-watch",
            daemon=True,
        )
        pm_membership_watch_thread.start()

    if broadcast_stats_thread is None or not broadcast_stats_thread.is_alive():
        broadcast_stats_thread = threading.Thread(
            target=broadcast_stats_monitor_loop,
            name="vk-broadcast-stats",
            daemon=True,
        )
        broadcast_stats_thread.start()

    if application_recovery_thread is None or not application_recovery_thread.is_alive():
        application_recovery_thread = threading.Thread(
            target=application_recovery_loop,
            name="vk-application-recovery",
            daemon=True,
        )
        application_recovery_thread.start()

    # Если прошлый процесс был убит хостингом во время /поискчаты,
    # продолжим с сохранённого запроса/страницы автоматически.
    _resume_interrupted_vkchats_search()

    for event in longpoll.listen():
        if auto_broadcast_thread is None or not auto_broadcast_thread.is_alive():
            auto_broadcast_thread = threading.Thread(target=auto_broadcast_loop, daemon=True)
            auto_broadcast_thread.start()
            logger.warning("Поток авторассылки перезапущен")
        if event.type == VkBotEventType.MESSAGE_EVENT:
            event_object = event.object
            payload = dict(event_object.get("payload") or {})
            payload["_event_object"] = event_object
            proxy_message = {
                "peer_id": event_object.get("peer_id"),
                "from_id": event_object.get("user_id"),
                "text": "",
                "payload": payload,
                "attachments": [],
                "_event_object": event_object,
            }
            event = type("CallbackEventProxy", (), {"obj": type("CallbackObj", (), {"message": proxy_message})()})()
        elif event.type != VkBotEventType.MESSAGE_NEW:
            continue

        message = event.obj.message
        if _is_duplicate_event(message):
            continue
        chat_id = message.get("peer_id")
        user_id = message.get("from_id")
        if user_id is None:
            user_id = message.get("user_id") or message.get("sender_id") or 0

        # Считаем ВСЕ callback-нажатия централизованно.
        raw_button_payload = message.get("payload")
        if isinstance(raw_button_payload, dict) and isinstance(
            raw_button_payload.get("_event_object"), dict
        ):
            try:
                record_button_click(
                    int(chat_id or 0),
                    int(user_id or 0),
                    raw_button_payload,
                )
            except Exception as exc:
                logger.warning("Не удалось записать статистику кнопки: %s", exc)

            # Меню статистики кнопок рассылки.
            if handle_button_stats_menu_action(
                int(chat_id or 0),
                int(user_id or 0),
                raw_button_payload,
            ):
                continue

            # Трековые URL-кнопки сначала считаются, затем открывают исходную ссылку.
            if handle_tracked_openlink_action(
                int(chat_id or 0),
                int(user_id or 0),
                raw_button_payload,
            ):
                continue

        text = (message.get("text") or "").strip()
        action = message.get("action")
        if isinstance(action, dict) and action.get("type") in {"chat_invite_user", "chat_invite_user_by_link"}:
            member_id = int(action.get("member_id") or 0)
            if abs(member_id) == group_id:
                registry = get_chat_registry()
                chat_title = registry.get(str(chat_id), {}).get("title") or "Без названия"
                log_invite(user_id, chat_id, chat_title)
                ensure_chat_registered(chat_id, announce=True)
            continue
        if isinstance(action, dict) and action.get("type") == "chat_title_update":
            # Событие Long Poll само содержит новое название. Это позволяет сохранить
            # его без административных прав, если переименование произошло при боте.
            event_title = str(action.get("text") or action.get("title") or "").strip()
            if event_title:
                cache_chat_title(chat_id, event_title, source="chat_title_update")
            else:
                request_chat_registry_refresh(chat_id, force=True)
            continue
        if isinstance(action, dict):
            continue

        # Заявки захватываются СРАЗУ после получения MESSAGE_NEW.
        # Поэтому последующие continue/обработчики команд больше не могут
        # случайно "съесть" обычное сообщение из беседы.
        if (
            int(chat_id or 0) >= 2_000_000_000
            and int(chat_id or 0) not in {int(admin_chat or 0), int(get_applications_chat() or 0)}
            and not bool(message.get("out"))
            and not _is_own_group_sender(user_id)
        ):
            _application_executor.submit(_capture_message_as_application, dict(message))

        command_text_for_chat = normalize_group_chat_command(text)
        command_text_lower = command_text_for_chat.lower()

        # Обрабатываем .устгород раньше регистрации/проверки беседы.
        # Команда не требует, чтобы сообщество было администратором чата;
        # проверяются только внутренние права пользователя в боте.
        if command_text_lower.startswith(".устгород"):
            if chat_id == user_id or not (len(str(chat_id)) == 10 and str(chat_id).startswith("2")):
                send_message(chat_id, "Команду .устгород нужно использовать в беседе.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав. Команда доступна администратору бота.")
                continue

            parts = command_text_for_chat.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Формат: .устгород <город>\nПример: .устгород Тамбов")
                continue

            raw_city = parts[1].strip()
            city_value = detect_city(raw_city) or raw_city
            city_value = city_value[:100].strip()
            if not city_value:
                send_message(chat_id, "Не удалось определить город. Пример: .устгород Тамбов")
                continue

            overrides = get_chat_city_overrides()
            overrides[str(chat_id)] = city_value
            runtime_data["chat_city_overrides"] = overrides
            save_runtime_data()

            send_message(
                chat_id,
                f"Данный чат установлен для приема рабочих заявок по городу: {city_value}\n"
                f"Ищете работу/рабочих? Переходите в чат - {WORK_CHAT_JOIN_LINK}",
                rate_limit=False,
            )
            update_user_stats(user_id, "command")
            logger.info(
                "Chat %s: администратор %s установил город заявок: %s",
                chat_id,
                user_id,
                city_value,
            )
            continue

        raw_chat_search_command = command_text_for_chat.strip()
        raw_chat_search_lower = raw_chat_search_command.lower()
        if (
            raw_chat_search_lower == "/поискчаты"
            or raw_chat_search_lower.startswith("/поискчаты ")
            or raw_chat_search_lower == ".поискчаты"
            or raw_chat_search_lower.startswith(".поискчаты ")
        ):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /поискчаты доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            requested_limit, exact_query, min_members = _parse_chat_search_command(raw_chat_search_command)
            if not start_vkchats_search(chat_id, requested_limit, exact_query, min_members):
                send_message(
                    chat_id,
                    render_vkchats_search_status(),
                    rate_limit=False,
                )
                continue

            update_user_stats(user_id, "command")
            continue

        if command_text_lower in {"/статакнопки", ".статакнопки"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ .статакнопки доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            show_button_stats_menu(chat_id, user_id, page=0)
            update_user_stats(user_id, "command")
            continue

        if (
            command_text_lower == "/статистиканажатий"
            or command_text_lower.startswith("/статистиканажатий ")
            or command_text_lower == ".статистиканажатий"
            or command_text_lower.startswith(".статистиканажатий ")
        ):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ .статистиканажатий доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            click_log_page = _parse_button_click_log_page(command_text_for_chat)
            send_long_message(
                chat_id,
                render_button_click_log(click_log_page),
            )
            update_user_stats(user_id, "command")
            continue

        if (
            command_text_lower == "/архивчаты"
            or command_text_lower.startswith("/архивчаты ")
            or command_text_lower == ".архивчаты"
            or command_text_lower.startswith(".архивчаты ")
        ):
            if chat_id != admin_chat:
                send_message(
                    chat_id,
                    "❌ .архивчаты доступна только в админ-чате.",
                )
                continue

            if not has_permission(user_id, "admin"):
                send_message(
                    chat_id,
                    "❌ Недостаточно прав.",
                )
                continue

            archive_page = _parse_archive_chats_page(
                command_text_for_chat
            )
            send_long_message(
                chat_id,
                render_found_chats_archive(archive_page),
            )
            update_user_stats(user_id, "command")
            continue

        if (
            command_text_lower == "/базачаты"
            or command_text_lower.startswith("/базачаты ")
            or command_text_lower == ".базачаты"
            or command_text_lower.startswith(".базачаты ")
        ):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ .базачаты доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            base_page = _parse_found_chats_page(command_text_for_chat)
            send_long_message(
                chat_id,
                render_found_chats_database(base_page),
            )
            update_user_stats(user_id, "command")
            continue

        if command_text_lower in {"/стоппоиск", ".стоппоиск"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /стоппоиск доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            if stop_vkchats_search():
                send_message(
                    chat_id,
                    "⏹ Останавливаю текущий поиск. "
                    "Если прямо сейчас идёт HTTP-запрос, остановка займёт до нескольких секунд.",
                    rate_limit=False,
                )
            else:
                send_message(chat_id, "ℹ️ Поиск чатов сейчас не запущен.", rate_limit=False)
            continue

        if command_text_lower in {"/автовход", ".автовход"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /автовход доступен только в админ-чате.")
                continue
            if user_id not in OWNER_IDS:
                send_message(chat_id, "❌ Автовход доступен только владельцам.")
                continue

            if not get_vk_user_token():
                send_message(
                    chat_id,
                    "❌ User token не найден. Вставьте его в INLINE_VK_USER_TOKEN в начале main.py или задайте VK_USER_TOKEN.",
                    rate_limit=False,
                )
                continue

            if start_autojoin(chat_id):
                send_message(
                    chat_id,
                    "✅ Автовход поставлен в фоновый поток.",
                    rate_limit=False,
                )
            else:
                send_message(
                    chat_id,
                    "ℹ️ Автовход уже работает.",
                    rate_limit=False,
                )
            update_user_stats(user_id, "command")
            continue

        if command_text_lower in {"/стопвход", ".стопвход"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /стопвход доступен только в админ-чате.")
                continue
            if user_id not in OWNER_IDS:
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            if stop_autojoin():
                send_message(chat_id, "⏹ Останавливаю автовход.", rate_limit=False)
            else:
                send_message(chat_id, "ℹ️ Автовход сейчас не запущен.", rate_limit=False)
            continue

        if command_text_lower in {"/повторвход", ".повторвход"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /повторвход доступен только в админ-чате.")
                continue
            if user_id not in OWNER_IDS:
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            removed = reset_autojoin_errors()
            send_message(
                chat_id,
                f"🔄 Сброшено ошибочных попыток: {removed}. Их можно попробовать снова через /автовход.",
                rate_limit=False,
            )
            continue

        if command_text_lower in {"/входчаты", ".входчаты"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /входчаты доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            start_manual_join_queue(chat_id)
            update_user_stats(user_id, "command")
            continue

        if command_text_lower in {"/панель", ".панель"}:
            if chat_id != admin_chat:
                send_message(chat_id, "❌ /панель доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Недостаточно прав.")
                continue

            # Быстро обновляем текущие read-счётчики перед открытием.
            try:
                refresh_broadcast_read_stats()
            except Exception as exc:
                logger.debug("Панель открыта с кэшированной статистикой: %s", exc)

            show_stats_panel(chat_id, user_id, period="latest")
            update_user_stats(user_id, "command")
            continue

        if text.strip().lower() == ".список":
            if chat_id != admin_chat:
                send_message(chat_id, f"❌ Эта команда доступна только в админ-чате.\nТекущий chat_id: {chat_id}\nadmin_chat: {admin_chat}")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, f"❌ У вас нет прав на выполнение этой команды.\nВаш role: {get_role(user_id) or 'user'}")
                continue
            send_long_message(chat_id, render_chat_list_text())
            update_user_stats(user_id, "command")
            continue

        payload = parse_message_payload(message, logger=logger)
        command = (payload.get("command") or text).strip().lower()
        if command.startswith("."):
            command = command[1:]
        plain_text_command = text.strip().lower()
        if plain_text_command.startswith("."):
            plain_text_command = plain_text_command[1:]
        is_personal_chat = chat_id == user_id

        # Панель статистики / просмотров.
        if handle_stats_panel_action(chat_id, user_id, payload):
            continue

        # Кнопка вакансии может быть нажата как в беседе, так и в личных сообщениях.
        # Сразу пробуем прислать карточку в ЛС; если нельзя — открываем диалог и ждём «Начать».
        if handle_job_delivery_action(chat_id, user_id, payload):
            continue

        # В личных сообщениях показывается отдельный каталог работодателя.
        # Для него не нужно читать крупные ads.json/orders.json на каждую кнопку.
        if is_personal_chat:
            mark_pm_activated(user_id)
            if handle_job_private_action(chat_id, user_id, payload):
                continue
            handle_job_private_message(user_id, text, message)
            continue

        # Рекламная база нужна только для бесед и административных команд.
        ads_data = order_service.load_ads()
        orders_data = order_service.load_orders()
        ensure_chat_registered(chat_id, announce=False)

        if text.strip().lower() == ".устзаявки":
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            if chat_id == admin_chat:
                send_message(chat_id, "Эту команду нужно написать в беседе, куда будут публиковаться подтверждённые объявления.")
                continue
            runtime_data["applications_chat"] = chat_id
            save_runtime_data()
            send_message(
                chat_id,
                "Чат публикации заявок установлен. Подтверждённые объявления будут находиться здесь 6 часов.",
            )
            update_user_stats(user_id, "command")
            continue

        if text.strip().lower() == ".заявки":
            if chat_id != admin_chat:
                send_message(chat_id, "Команда .заявки доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue

            configured_chat = get_applications_chat()
            with applications_lock:
                app_data = load_applications()
                pending_items = [item for item in app_data["items"] if item.get("status") == "pending"]
                published_count = sum(1 for item in app_data["items"] if item.get("status") == "published")

            if not pending_items:
                send_message(
                    chat_id,
                    f"Новых заявок нет.\nЧат публикации: {configured_chat or 'не установлен'}\nСейчас опубликовано: {published_count}",
                )
                continue

            send_message(
                chat_id,
                f"Заявок на проверке: {len(pending_items)}\nЧат публикации: {configured_chat or 'не установлен'}\nПоказываю последние 20.",
            )
            for item in pending_items[-20:]:
                send_message(
                    chat_id,
                    render_application_text(item),
                    keyboard=build_application_keyboard(int(item.get("id", 0) or 0)),
                    rate_limit=False,
                )
            update_user_stats(user_id, "command")
            continue

        if handle_application_action(chat_id, user_id, payload):
            continue

        if handle_pm_job_admin_action(chat_id, user_id, payload):
            continue

        if handle_main_button_edit_action(chat_id, user_id, payload):
            continue

        if handle_jobs_broadcast_edit_action(chat_id, user_id, payload):
            continue

        if handle_admin_order_action(chat_id, user_id, payload):
            continue

        text_lower = text.strip().lower()

        if text_lower == ".инфозов":
            if chat_id != admin_chat:
                send_message(chat_id, "Команда .инфозов доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав. Команда доступна администратору бота.")
                continue
            with infozov_lock:
                if infozov_thread is not None and infozov_thread.is_alive():
                    send_message(chat_id, "Информационная рассылка уже выполняется. Дождитесь её завершения.")
                    continue
                infozov_thread = threading.Thread(
                    target=run_infozov_broadcast,
                    args=(chat_id, user_id),
                    name="infozov-broadcast",
                    daemon=True,
                )
                infozov_thread.start()
            new_jobs, _ = _infozov_new_jobs()
            send_message(
                chat_id,
                f"Запускаю рассылку по пользователям ЛС. Новых активных вакансий: {len(new_jobs)}.",
                rate_limit=False,
            )
            continue

        if text_lower.startswith(".добвакрассылка") or text_lower.startswith(".добоснвак"):
            if chat_id != admin_chat:
                send_message(chat_id, "Команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(
                    chat_id,
                    "Формат: .добвакрассылка <текст>\n"
                    "К этому же сообщению можно прикрепить фото или видео.\n"
                    "Кнопка «Посмотреть вакансии» и ссылка на меню добавятся автоматически.",
                )
                continue
            block = save_jobs_broadcast_message_from_admin(parts[1], message, user_id)
            attachment, changed = resolve_jobs_broadcast_attachment(block)
            if changed:
                with _runtime_data_lock:
                    runtime_data["jobs_broadcast_message"] = block
                    save_runtime_data()
            send_message(
                chat_id,
                "✅ Компактный блок вакансий сохранён. Он будет отправляться один раз "
                "в общей рассылке прямо перед основным сообщением.\n\n"
                f"Ссылка на меню вакансий: {build_pm_jobs_catalog_deep_link()}\n\n"
                + str(block.get("text") or ""),
                attachment=attachment,
                keyboard=build_jobs_broadcast_keyboard(),
            )
            update_user_stats(user_id, "command")
            continue

        if text_lower in {".редвак", ".редвакрассылка"}:
            if chat_id != admin_chat or not has_permission(user_id, "admin"):
                send_message(chat_id, "Команда доступна только администраторам в админ-чате.")
                continue
            block = get_jobs_broadcast_message()
            if not block.get("enabled") or not str(block.get("text") or "").strip():
                send_message(chat_id, "Компактный блок вакансий ещё не настроен. Сначала используйте .добвакрассылка <текст>")
                continue
            attachment, changed = resolve_jobs_broadcast_attachment(block)
            if changed:
                with _runtime_data_lock:
                    runtime_data["jobs_broadcast_message"] = block
                    save_runtime_data()
            preview_message_id = send_message(
                chat_id,
                str(block.get("text") or ""),
                attachment=attachment,
                keyboard=build_jobs_broadcast_keyboard(),
            )
            send_message(
                chat_id,
                "Действия с блоком вакансий:",
                keyboard=build_jobs_broadcast_edit_keyboard(
                    user_id,
                    preview_message_id if isinstance(preview_message_id, int) else None,
                ),
            )
            update_user_stats(user_id, "command")
            continue

        if text_lower in {".вакрассылка", ".оснвак"}:
            if chat_id != admin_chat or not has_permission(user_id, "admin"):
                send_message(chat_id, "Команда доступна только администраторам в админ-чате.")
                continue
            block = get_jobs_broadcast_message()
            if not block.get("enabled") or not str(block.get("text") or "").strip():
                send_message(chat_id, "Компактный блок вакансий ещё не настроен. Используйте .добвакрассылка <текст>")
                continue
            attachment, _changed = resolve_jobs_broadcast_attachment(block)
            send_message(
                chat_id,
                str(block.get("text") or ""),
                attachment=attachment,
                keyboard=build_jobs_broadcast_keyboard(),
            )
            continue

        if text_lower in {".удвакрассылка", ".удоснвак"}:
            if chat_id != admin_chat or not has_permission(user_id, "admin"):
                send_message(chat_id, "Команда доступна только администраторам в админ-чате.")
                continue
            delete_jobs_broadcast_message()
            send_message(chat_id, "✅ Компактный блок вакансий удалён из общей рассылки.")
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".создатькомпанию"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Формат: .создатькомпанию <Название>")
                continue
            company, error = create_company(parts[1], user_id)
            if error:
                send_message(chat_id, error)
                continue
            send_message(
                chat_id,
                f"Компания создана: №{company.get('id')} — {company.get('name')}\n"
                "Она видна только администраторам и используется при добавлении работ.",
            )
            update_user_stats(user_id, "command")
            continue

        if text_lower == ".компании":
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            send_long_message(chat_id, render_companies_admin_list())
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".удкомпания"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_long_message(chat_id, render_companies_admin_list())
                continue
            removed_company = delete_company(parts[1])
            if not removed_company:
                send_message(chat_id, "Компания не найдена. Используйте .компании.")
                continue
            linked_jobs = [
                job for job in get_active_pm_jobs()
                if int(job.get("company_id", 0) or 0) == int(removed_company.get("id", 0) or 0)
            ]
            suffix = (
                f"\nСвязанные работы сохранены: {len(linked_jobs)}. В них останется записанное название компании."
                if linked_jobs else ""
            )
            send_message(
                chat_id,
                f"Компания удалена: №{removed_company.get('id')} — {removed_company.get('name')}" + suffix,
            )
            update_user_stats(user_id, "command")
            continue

        if text_lower == ".добработа":
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            if not get_companies():
                send_message(
                    chat_id,
                    "Сначала создайте компанию командой:\n.создатькомпанию <Название>",
                )
                continue
            USER_STATES.setdefault(str(user_id), {})["pm_job_create"] = {
                "step": "company",
                "chat_id": chat_id,
            }
            send_pm_company_selection(chat_id, 0)
            update_user_stats(user_id, "command")
            continue

        if text_lower in {".работы", ".списокработ"}:
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            send_long_message(chat_id, render_pm_jobs_admin_list())
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".редрабфото"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue

            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_long_message(
                    chat_id,
                    render_pm_jobs_admin_list()
                    + "\n\nФормат: .редрабфото <номер или точное название> + приложить фото к этому же сообщению",
                )
                continue

            job = find_pm_job(parts[1], active_only=False)
            if not job:
                send_message(chat_id, "Работа не найдена. Используйте .работы, чтобы посмотреть номера.")
                continue

            photo_attachments = [
                raw
                for raw in (message.get("attachments") or [])
                if isinstance(raw, dict) and raw.get("type") == "photo"
            ]
            if not photo_attachments:
                send_message(
                    chat_id,
                    f"Приложите фото к этому же сообщению.\n\n"
                    f"Например: .редрабфото {int(job.get('id', 0) or 0)} + фото",
                )
                continue

            photo_message = {"attachments": photo_attachments}
            attachments, attachment_files = prepare_pm_job_attachments(
                photo_message,
                prefix=(
                    f"pm_job_quick_photo_{int(job.get('id', 0) or 0)}_"
                    f"{int(user_id)}_{int(time.time() * 1000)}"
                ),
            )
            if not attachments:
                send_message(chat_id, "Не удалось сохранить фото. Попробуйте отправить его ещё раз.")
                continue

            job_id = int(job.get("id", 0) or 0)
            updated = update_pm_job(
                job_id,
                {
                    "attachment": ",".join(attachments),
                    "attachment_files": attachment_files,
                },
            )
            if not updated:
                send_message(chat_id, "Не удалось обновить вакансию.")
                continue

            send_message(
                chat_id,
                f"✅ Фото вакансии №{job_id} обновлено.\n"
                f"{updated.get('title') or f'Работа №{job_id}'}\n"
                f"Фото: {len(attachments)} шт.",
                attachment=",".join(attachments),
            )
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".редработа") or text_lower.startswith(".измработа"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_long_message(
                    chat_id,
                    render_pm_jobs_admin_list() + "\n\nФормат: .измработа <номер или точное название>",
                )
                continue
            job = find_pm_job(parts[1])
            if not job:
                send_message(chat_id, "Работа не найдена. Используйте .работы, чтобы посмотреть номера.")
                continue
            USER_STATES.setdefault(str(user_id), {}).pop("pm_job_create", None)
            USER_STATES.setdefault(str(user_id), {})["pm_job_edit"] = {
                "step": "title",
                "chat_id": chat_id,
                "job_id": int(job.get("id", 0) or 0),
            }
            send_message(
                chat_id,
                f"Редактирование работы №{job.get('id')}.\n"
                f"Текущая кнопка: {job.get('title')}\n\n"
                "Отправьте новое название кнопки.\n"
                ".оставить — не менять\n"
                ".отмена — отменить редактирование",
            )
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".удработа"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "Недостаточно прав.")
                continue
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_long_message(chat_id, render_pm_jobs_admin_list())
                continue
            removed = delete_pm_job(parts[1])
            if not removed:
                send_message(chat_id, "Работа не найдена. Используйте .работы, чтобы посмотреть номера.")
                continue
            send_message(
                chat_id,
                f"Работа удалена и больше не показывается в ЛС:\n"
                f"№{removed.get('id')} — {removed.get('title')}",
            )
            update_user_stats(user_id, "command")
            continue

        if handle_pm_job_edit_input(chat_id, user_id, text, message):
            continue

        if handle_pm_job_admin_input(chat_id, user_id, text, message):
            continue

        if handle_main_button_edit_input(chat_id, user_id, text):
            continue

        if handle_jobs_broadcast_edit_input(chat_id, user_id, text, message):
            continue

        if handle_admin_order_edit_input(chat_id, user_id, text, message):
            continue

        if handle_admin_text_command(chat_id, user_id, text, message):
            continue

        if text == ".пинг":
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            start_time = time.time()
            send_message(chat_id, "Проверка пинга...")
            end_time = time.time()
            ping_time = int((end_time - start_time) * 1000)
            send_message(chat_id, f"Пинг: {ping_time}ms")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".стата"):
            target_user_id = extract_target_user(message, text) or user_id
            if target_user_id != user_id and (chat_id != admin_chat or not has_permission(user_id, "admin")):
                send_message(chat_id, "❌ Обычный пользователь может смотреть только свою статистику.")
                continue
            send_message(chat_id, render_user_stats_detailed(target_user_id))
            update_user_stats(user_id, "command")
            continue

        if text == ".хелп":
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            role = get_role(user_id)
            send_message(chat_id, get_help_text(role))
            update_user_stats(user_id, "command")
            continue

        if command == "список" or plain_text_command == "список":
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Команда списка чатов доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ У вас нет прав на просмотр списка чатов.")
                continue
            send_long_message(chat_id, render_chat_list_text())
            update_user_stats(user_id, "command")
            continue

        if command == "обновитьчаты" or plain_text_command == "обновитьчаты":
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            target_chats = [cid for cid in chat_ids if cid != admin_chat]
            if not target_chats:
                send_message(chat_id, "⚠️ Список чатов рассылки пуст.")
                continue
            send_message(chat_id, f"🔄 Обновляю данные чатов...\nВсего чатов: {len(target_chats)}")
            def _do_refresh_chats():
                updated = 0
                for cid in target_chats:
                    try:
                        refresh_chat_registry_entry(cid)
                        updated += 1
                    except Exception:
                        pass
                send_message(chat_id, f"✅ Обновление завершено.\nОбновлено чатов: {updated}/{len(target_chats)}")
            threading.Thread(target=_do_refresh_chats, daemon=True).start()
            update_user_stats(user_id, "command")
            continue


        if text == ".ид" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(chat_id, f"✅ ID этой беседы: {chat_id}")
            update_user_stats(user_id, "command")
            continue

        if text == ".инфо" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(chat_id, render_runtime_info_legacy())
            update_user_stats(user_id, "command")
            continue

        if text == ".допсписок" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(chat_id, render_additional_texts_list())
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".редтекст") and chat_id == admin_chat and has_permission(user_id, "admin"):
            parts = text.split(" ", 2)
            if len(parts) < 3:
                send_message(chat_id, "Использование: .редтекст [номер] [текст]")
                continue
            try:
                text_number = int(parts[1])
                if text_number < 1:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Номер должен быть положительным числом.")
                continue
            additional_texts = get_additional_texts()
            index = text_number - 1
            if index >= len(additional_texts):
                send_message(chat_id, f"Текст с номером {text_number} не существует.")
                continue
            additional_texts[index] = parts[2].strip()
            save_additional_texts(additional_texts)
            send_message(chat_id, f"✅ Дополнительный текст №{text_number} обновлён.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".добфото"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(" ", 1)
            if len(parts) < 2:
                send_message(chat_id, "Укажите номер текста. Пример: .добфото 1")
                continue
            try:
                text_number = int(parts[1])
                if text_number < 1:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Номер должен быть числом. Пример: .добфото 1")
                continue
            additional_texts = get_additional_texts()
            index = text_number - 1
            if index >= len(additional_texts):
                send_message(chat_id, f"Текст с номером {text_number} не существует.")
                continue
            attachments = extract_attachment_references(message)
            if not attachments:
                send_message(chat_id, "❌ Прикрепите фото к сообщению с командой .добфото")
                continue
            additional_photos = get_additional_photos()
            additional_photos.setdefault(str(index), [])
            additional_photos[str(index)].extend(attachments)
            save_additional_texts(additional_texts, additional_photos)
            send_message(chat_id, f"✅ Фото успешно добавлены к доп. тексту №{text_number}", attachment=",".join(attachments))
            update_user_stats(user_id, "osn_photo")
            continue

        if text.startswith(".удфото"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(" ", 1)
            if len(parts) < 2:
                send_message(chat_id, "Укажите номер текста. Пример: .удфото 1")
                continue
            try:
                text_number = int(parts[1])
                if text_number < 1:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Номер должен быть числом. Пример: .удфото 1")
                continue
            additional_texts = get_additional_texts()
            index = text_number - 1
            if index >= len(additional_texts):
                send_message(chat_id, f"Текст с номером {text_number} не существует.")
                continue
            additional_photos = get_additional_photos()
            if str(index) in additional_photos:
                del additional_photos[str(index)]
                save_additional_texts(additional_texts, additional_photos)
                send_message(chat_id, f"🗑️ Все фото для доп. текста №{text_number} удалены")
            else:
                send_message(chat_id, f"ℹ️ У доп. текста №{text_number} не было прикреплённых фото")
            update_user_stats(user_id, "command")
            continue

        if text in {".активрекламы", ".списокрекламы"} and chat_id == admin_chat and has_permission(user_id, "admin"):
            active_ads = iter_active_ads()
            if not active_ads:
                send_message(chat_id, "Сейчас нет активных рекламных объявлений.")
                continue
            for index, (ad_key, ad_data) in enumerate(active_ads, start=1):
                user_label = render_user_mention(int(ad_data.get("user_id"))) if ad_data.get("user_id") else ad_key
                expires_at = ad_data.get("expires_at", "не указано")
                text_value = ad_data.get("text", "") or "Текст не указан"
                preview = text_value if len(text_value) <= 700 else f"{text_value[:700]}..."
                related_order_id = ad_data.get("order_id") or (ad_key if ad_key in orders_data else None)
                order_code = (
                    str(ad_data.get("order_code", "") or "").upper()
                    or (get_order_display_code(related_order_id) if related_order_id else "")
                    or "НЕ УКАЗАН"
                )
                ad_type = "основная" if ad_key == "main_text_sale" or ad_data.get("type") == "main_text" else "дополнительная"
                broadcast_state = "приостановлена" if ad_data.get("broadcast_paused") else "в рассылке"
                send_message(
                    chat_id,
                    f"Реклама #{index}\nКод: {order_code}\nТип: {ad_type}\nСостояние: {broadcast_state}\nПользователь: {user_label}\nАктивна до: {expires_at}\n\n{preview}",
                    attachment=ad_data.get("photo"),
                )
            send_message(chat_id, f"Всего активных объявлений: {len(active_ads)}")
            update_user_stats(user_id, "command")
            continue

        if command == "уст" and has_permission(user_id, "admin"):
            if admin_chat is None:
                send_message(chat_id, "Admin chat is not set.")
                continue
            if chat_id != admin_chat:
                send_message(chat_id, "This command is available only in the admin chat.")
                continue
            if len(str(chat_id)) == 10 and str(chat_id).startswith("2"):
                if ensure_chat_registered(chat_id, announce=True):
                    send_message(chat_id, "Chat added to broadcast base. Bot message with button was sent.")
                else:
                    request_chat_registry_refresh(chat_id, force=True)
                    send_message(chat_id, "Chat already exists in the base. Metadata refresh started in background.")
            else:
                send_message(chat_id, "Cannot add this chat: it is not a conversation.")
            update_user_stats(user_id, "command")
            continue

        if text == ".инфочат" and chat_id == admin_chat and has_permission(user_id, "admin"):
            send_message(
                chat_id,
                "📋 Информация о чате:\n"
                "\n"
                f"🔹 ID чата: {chat_id}\n"
                f"🔹 ID отправителя: {user_id}\n"
                f"🔹 Режим Long Poll: {'Включён' if longpoll_enabled else 'Отключён или не настроен'}\n"
                f"🔹 Текущий административный чат: {admin_chat if admin_chat else 'Не установлен'}\n"
                "▫️ Версия бота: 2.0\n",
            )
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".добид") and chat_id == admin_chat and has_permission(user_id, "admin"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                send_message(chat_id, "Использование: .добид [число]")
                continue
            try:
                count = int(parts[1])
                if count <= 0:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Количество должно быть положительным числом.")
                continue
            bot_chat_ids = [cid for cid in chat_ids if str(cid).startswith("2") and len(str(cid)) == 10]
            next_chat_id = max(bot_chat_ids, default=2000000000) + 1
            for _ in range(count):
                chat_ids.append(next_chat_id)
                next_chat_id += 1
            save_runtime_data()
            send_message(chat_id, f"✅ Добавлено {count} чатов в список.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".делид") and chat_id == admin_chat and has_permission(user_id, "admin"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                send_message(chat_id, "Использование: .делид [число]")
                continue
            try:
                count = int(parts[1])
                if count <= 0:
                    raise ValueError
            except ValueError:
                send_message(chat_id, "Количество должно быть положительным числом.")
                continue
            bot_chat_ids = [cid for cid in chat_ids if str(cid).startswith("2") and len(str(cid)) == 10]
            removed = 0
            for _ in range(min(count, len(bot_chat_ids))):
                if bot_chat_ids:
                    chat_to_remove = bot_chat_ids.pop()
                    if chat_to_remove in chat_ids:
                        chat_ids.remove(chat_to_remove)
                        removed += 1
            if removed:
                save_runtime_data()
            send_message(chat_id, f"✅ Удалено {removed} чатов с конца списка.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".оффрассылка") and has_permission(user_id, "dev"):
            parts = text.split(maxsplit=1)
            if len(parts) >= 2:
                target = parts[1].strip()
                try:
                    target_chat_id = int(target)
                except ValueError:
                    send_message(chat_id, "❌ Укажите числовой ID чата.")
                    continue
            else:
                target_chat_id = chat_id
            if target_chat_id not in chat_ids:
                send_message(chat_id, f"❌ Чат {target_chat_id} не найден в списке рассылки.")
                continue
            if target_chat_id == admin_chat:
                send_message(chat_id, "❌ Нельзя удалить административный чат из рассылки.")
                continue
            chat_ids.remove(target_chat_id)
            save_runtime_data()
            registry = get_chat_registry()
            entry = registry.get(str(target_chat_id), {})
            title = entry.get("title") or "Без названия"
            send_message(chat_id, f"✅ Чат «{title}» (ID: {target_chat_id}) удалён из списка рассылки.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".отправить") and has_permission(user_id, "dev"):
            parts = text.split(maxsplit=2)
            if len(parts) < 3:
                send_message(chat_id, "❌ Формат: .отправить [chat_id] [текст]")
                continue
            try:
                target_chat_id = int(parts[1])
            except ValueError:
                send_message(chat_id, "❌ ID чата должен быть числом.")
                continue
            message_text = parts[2]
            photo_ref = extract_attachment_reference(message or {})
            send_message(target_chat_id, message_text, attachment=photo_ref)
            send_message(chat_id, f"✅ Сообщение отправлено в чат {target_chat_id}.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".логиприглашения") and has_permission(user_id, "dev"):
            log = load_json_file(INVITES_LOG_PATH, [])
            parts = text.split(maxsplit=1)
            if len(parts) >= 2:
                token = parts[1].strip()
                target_uid = resolve_user_id(token)
                if not target_uid:
                    send_message(chat_id, "❌ Не удалось определить пользователя.")
                    continue
                filtered = [e for e in log if e.get("user_id") == target_uid]
                name = get_user_display_name(target_uid)
                header = f"📋 Приглашения от {name} (ID: {target_uid}):\n\n"
            else:
                filtered = log
                header = f"📋 Все приглашения бота ({len(filtered)}):\n\n"
            if not filtered:
                send_message(chat_id, header + "Нет записей.")
                update_user_stats(user_id, "command")
                continue
            lines = []
            for entry in filtered[-30:]:
                inviter = get_user_display_name(int(entry.get("user_id") or 0))
                title = entry.get("chat_title", "Без названия")
                cid = entry.get("chat_id", "?")
                when = entry.get("created_at", "?")
                confirmed = "✅" if entry.get("confirmed") else "⏳"
                lines.append(f"• {when} {confirmed} — {inviter} → «{title}» (ID: {cid})")
            send_long_message(chat_id, header + "\n".join(lines))
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".обновитназвания") and has_permission(user_id, "dev"):
            send_message(chat_id, "⏳ Обновляю названия чатов...")
            def _do_refresh():
                updated, failed = refresh_all_chat_titles()
                send_message(chat_id, f"✅ Готово. Обновлено: {updated}, ошибок: {failed}")
            threading.Thread(target=_do_refresh, daemon=True).start()
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".тестчат") and has_permission(user_id, "dev"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                send_message(chat_id, "Формат: .тестчат [chat_id]")
                continue
            try:
                test_cid = int(parts[1])
            except ValueError:
                send_message(chat_id, "chat_id должен быть числом.")
                continue
            # Тест getConversationsById
            try:
                r1 = vk.messages.getConversationsById(peer_ids=test_cid)
                items1 = r1.get("items", [])
                if items1:
                    cs = (items1[0] or {}).get("chat_settings") or {}
                    send_message(chat_id, f"getConversationsById: title='{cs.get('title')}', invite_link={cs.get('invite_link')}")
                else:
                    send_message(chat_id, f"getConversationsById: пустой items")
            except Exception as e:
                send_message(chat_id, f"getConversationsById ошибка: {e}")
            # Тест getConversations
            try:
                r2 = vk.messages.getConversations(peer_ids=[test_cid], extended=1)
                items2 = r2.get("items", [])
                if items2:
                    peer = (items2[0].get("conversation") or {}).get("peer") or {}
                    send_message(chat_id, f"getConversations: title='{peer.get('title')}', peer={peer}")
                else:
                    send_message(chat_id, f"getConversations: пустой items, keys={list(r2.keys())}")
            except Exception as e:
                send_message(chat_id, f"getConversations ошибка: {e}")
            update_user_stats(user_id, "command")
            continue

        if text == ".доходы" and has_permission(user_id, "dev"):
            send_message(chat_id, render_income_stats())
            update_user_stats(user_id, "command")
            continue

        if text == ".тест" and has_permission(user_id, "admin"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            test_items = build_vk_full_broadcast_items()

            # Как в старой рабочей версии: не блокируем video attachment
            # дополнительными getById/forward/mp4-проверками.
            _background_executor.submit(
                lambda target_chat_id=chat_id, ready_items=test_items: send_broadcast_to_chat(
                    target_chat_id, ready_items
                )
            )
            send_message(
                chat_id,
                "✅ Тестовая отправка запущена старым способом: сохранённое VK-видео прикрепляется напрямую.",
                rate_limit=False,
            )
            update_user_stats(user_id, "command")
            continue

        text_lower = text.lower()

        if text_lower.startswith(".стопзаказ"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Формат: .стопзаказ <номер заказа>")
                continue
            _, response_text = set_order_broadcast_paused(parts[1].strip(), True, user_id)
            send_message(chat_id, response_text)
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".стартзаказ"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Формат: .стартзаказ <номер заказа>")
                continue
            _, response_text = set_order_broadcast_paused(parts[1].strip(), False, user_id)
            send_message(chat_id, response_text)
            update_user_stats(user_id, "command")
            continue

        if text_lower.startswith(".отпрзаказ"):
            if chat_id != admin_chat:
                send_message(chat_id, "Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "admin"):
                send_message(chat_id, "У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Формат: .отпрзаказ <номер заказа>")
                continue
            order_ref = parts[1].strip()
            order_id, ad_key, order, ad_data = resolve_ad_for_order_reference(order_ref)
            if not ad_key or not isinstance(ad_data, dict):
                send_message(chat_id, "Заказ не найден среди активных рекламных сообщений.")
                continue
            code = str(ad_data.get("order_code") or get_order_display_code(order_id or order_ref, order)).upper()
            if start_single_order_broadcast(order_ref, notify_chat_id=chat_id):
                send_message(chat_id, f"Запущена разовая отправка только заказа {code}.")
                update_user_stats(user_id, "command")
            continue

        if text == ".рассылка" and has_permission(user_id, "admin"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.", rate_limit=False)
                continue
            if start_broadcast(notify_chat_id=chat_id):
                last_broadcast_time = time.time()
                send_message(
                    chat_id,
                    "✅ Рассылка запущена в фоне. Команды, ЛС и кнопки продолжают работать.",
                    rate_limit=False,
                )
                update_user_stats(user_id, "command")
            else:
                send_message(
                    chat_id,
                    "ℹ️ Новая общая рассылка не запущена: предыдущая ещё выполняется или действует ночной запрет.",
                    rate_limit=False,
                )
            continue

        if text == ".gzov" and has_permission(user_id, "dev"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.", rate_limit=False)
                continue
            if start_gzov(notify_chat_id=chat_id):
                send_message(
                    chat_id,
                    "✅ GZOV запущен в фоне. Команды, ЛС и кнопки продолжают работать.",
                    rate_limit=False,
                )
                update_user_stats(user_id, "command")
            else:
                send_message(chat_id, "ℹ️ GZOV уже выполняется.", rate_limit=False)
            continue

        if text == ".заказы":
            if chat_id != admin_chat or not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Команда доступна только администрации в админ-чате.")
                continue
            orders_keyboard = build_orders_list_keyboard(orders_data)
            send_message(chat_id, render_orders_list_text(orders_data), keyboard=orders_keyboard)
            continue

        if text == ".историязаказов":
            if chat_id != admin_chat or not has_permission(user_id, "admin"):
                send_message(chat_id, "❌ Команда доступна только администрации в админ-чате.")
                continue
            history_keyboard = build_orders_history_keyboard(orders_data, page=0)
            send_message(chat_id, render_orders_history_text(orders_data, page=0), keyboard=history_keyboard)
            continue

        if text == ".инфо_о_нас" or payload.get("command") == ".инфо_о_нас":
            handle_group_info(chat_id)
            continue

        if text == ".меню":
            active_ad = order_service.get_user_active_ad(ads_data, user_id)
            has_messages = any(order.get("user_id") == user_id and order.get("type") != "main_text" for order in orders_data.values())
            send_message(
                chat_id,
                "Главное меню",
                keyboard=generate_main_menu(has_active_ad=bool(active_ad), has_messages=has_messages or bool(active_ad)),
            )
            continue

        if text == ".админчат":
            if not has_permission(user_id, "dev"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            if admin_chat == chat_id:
                send_message(chat_id, "⚠️ Этот чат уже является административным.")
            else:
                # Удаляем старый админ-чат из списка рассылки
                old_admin = admin_chat
                admin_chat = chat_id
                if old_admin is not None and old_admin in chat_ids:
                    chat_ids.remove(old_admin)
                    logger.info(f"Старый админ-чат {old_admin} удалён из рассылки")
                save_runtime_data()
                send_message(chat_id, "Административный чат установлен.")

        if text == ".стафф":
            if user_id in OWNER_IDS:
                send_message(chat_id, render_staff_detailed())
            else:
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
            continue

        if text == ".редоснкнопка":
            if not has_permission(user_id, "dev"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            send_message(
                chat_id,
                f"⚙️ Настройка кнопки основного сообщения\n\n"
                f"Название: {main_button_title}\n"
                f"Ссылка: {main_button_link}",
                keyboard=build_main_button_edit_keyboard(),
            )
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".редоснтекст") and has_permission(user_id, "dev"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                continue
            if not has_permission(user_id, "dev"):
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            parts = text.split(" ", 1)
            if len(parts) < 2 or not parts[1].strip():
                send_message(chat_id, "Неверный формат команды. Используйте: .редоснтекст [текст]")
                continue
            message_text = parts[1].strip()
            save_runtime_data()
            send_message(chat_id, message_text, attachment=upload_message_photo())
            update_user_stats(user_id, "osn_text")
            continue

        if text.startswith(".настройки") and chat_id == admin_chat and has_permission(user_id, "dev"):
            parts = text.split()
            if len(parts) != 3:
                send_message(chat_id, "❌ Неверный формат. Используйте: .настройки [cd_min|interval_sec|service_send_cooldown_sec] [число]")
                continue
            key = parts[1]
            try:
                value = float(parts[2])
            except ValueError:
                send_message(chat_id, "❌ Значение должно быть числом.")
                continue
            if key == "cd_min":
                if value < 1 or value > 1440:
                    send_message(chat_id, "❌ КД должно быть от 1 до 1440 минут.")
                    continue
                cd_min = int(value)
                save_config_value("cd_min", str(cd_min))
                send_message(chat_id, f"✅ Установлено: cd_min = {cd_min} мин")
            elif key == "interval_sec":
                if value < 0 or value > 60:
                    send_message(chat_id, "❌ Интервал должен быть от 0 до 60 секунд.")
                    continue
                interval_sec = float(value)
                save_config_value("interval_sec", str(interval_sec))
                send_message(chat_id, f"✅ Установлено: interval_sec = {interval_sec} сек")
            elif key == "service_send_cooldown_sec":
                if value < 0 or value > 60:
                    send_message(chat_id, "❌ КД обычных сообщений должен быть от 0 до 60 секунд.")
                    continue
                service_send_cooldown_sec = float(value)
                save_runtime_data()
                send_message(chat_id, f"✅ Установлено: service_send_cooldown_sec = {service_send_cooldown_sec} сек")
            else:
                send_message(chat_id, "❌ Доступные ключи: cd_min, interval_sec, service_send_cooldown_sec")
                continue
            update_user_stats(user_id, "command")
            continue

        if text == ".редоснфото":
            if user_id in OWNER_IDS:
                if chat_id != admin_chat:
                    send_message(chat_id, "❌ Эта команда доступна только в админ-чате.")
                    continue
                ok, result_text = save_main_photo_from_message(message)
                send_message(chat_id, result_text)
                if ok:
                    update_user_stats(user_id, "osn_photo")
            else:
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
            continue

        if text.startswith(".админ"):
            if chat_id != admin_chat or not has_permission(user_id, "dev"):
                send_message(chat_id, "❌ Команда доступна только разработчику в админ-чате.")
                continue
            target_user_id = extract_target_user(message, text)
            if not target_user_id:
                send_message(chat_id, "❌ Укажите пользователя: ответом, @ или ссылкой.")
                continue
            if user_id == target_user_id:
                send_message(chat_id, "❌ Нельзя снимать права администратора с себя через эту команду.")
                continue
            users = load_users()
            user_entry = users.setdefault(
                str(target_user_id),
                {"role": "user", "osn_photo_count": 0, "osn_text_count": 0, "total_messages": 0, "last_message": ""},
            )
            current_role = user_entry.get("role")
            if current_role == "admin":
                user_entry["role"] = "user"
                save_users(users)
                send_message(chat_id, f"❌ Права администратора сняты у пользователя {target_user_id}.")
            else:
                user_entry["role"] = "admin"
                save_users(users)
                send_message(chat_id, f"✅ Пользователь {target_user_id} назначен администратором.")
            update_user_stats(user_id, "command")
            continue

        if text.startswith(".разраб"):
            if chat_id != admin_chat:
                send_message(chat_id, "❌ Команда доступна только в админ-чате.")
                continue
            if user_id not in OWNER_IDS:
                send_message(chat_id, "❌ У вас нет прав на выполнение этой команды.")
                continue
            target_user_id = extract_target_user(message, text)
            if not target_user_id:
                send_message(chat_id, "❌ Укажите пользователя: ответом, @ или ссылкой.")
                continue
            users = load_users()
            user_key = str(target_user_id)
            current_role = users.get(user_key, {}).get("role", "user")
            if current_role == "dev":
                users.setdefault(user_key, {"role": "user", "osn_photo_count": 0, "osn_text_count": 0, "total_messages": 0, "last_message": ""})
                users[user_key]["role"] = "user"
                save_users(users)
                try:
                    user_info = vk.users.get(user_ids=target_user_id)[0]
                    full_name = f"{user_info['first_name']} {user_info['last_name']}"
                    send_message(chat_id, f"❌ Права разработчика сняты у [id{target_user_id}|{full_name}]")
                except Exception as e:
                    send_message(chat_id, f"❌ Права разработчика сняты у пользователя {target_user_id}. Произошла ошибка при получении имени: {e}")
            else:
                users.setdefault(user_key, {"role": "user", "osn_photo_count": 0, "osn_text_count": 0, "total_messages": 0, "last_message": ""})
                users[user_key]["role"] = "dev"
                save_users(users)
                try:
                    user_info = vk.users.get(user_ids=target_user_id)[0]
                    full_name = f"{user_info['first_name']} {user_info['last_name']}"
                    send_message(chat_id, f"✅ [id{target_user_id}|{full_name}] назначен(а) разработчиком.")
                except Exception as e:
                    send_message(chat_id, f"✅ Пользователь {target_user_id} назначен разработчиком. Произошла ошибка при получении имени: {e}")
            continue

if __name__ == "__main__":
    main()










