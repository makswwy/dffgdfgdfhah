from __future__ import annotations

import asyncio
import html
import logging
import os
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import vk_api
from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ChatMemberHandler,
    TypeHandler,
    filters,
)

from config import group_token
from shared_state import APPLICATIONS_LOCK, PM_JOBS_LOCK, TELEGRAM_USERS_LOCK, TELEGRAM_BROADCAST_LOCK
from utils.db import ensure_json_file, read_json, write_json_atomic


LOGGER = logging.getLogger("vk_bot.telegram")
MSK = timezone(timedelta(hours=3))
PM_JOBS_PAGE_SIZE = 5
APPLICATION_LIFETIME_SECONDS = 6 * 60 * 60
TELEGRAM_BROADCAST_EVENT_TTL_SECONDS = 60 * 60
TELEGRAM_BROADCAST_QUEUE_FILE = "telegram_broadcast_queue.json"


def _clean_broadcast_item(item: dict[str, Any]) -> dict[str, Any]:
    files = item.get("photo_files") or []
    if not isinstance(files, list):
        files = [files]
    return {
        "text": str(item.get("text") or ""),
        "photo_refs": str(item.get("photo_refs") or ""),
        "photo_files": [str(value) for value in files if str(value or "").strip()],
        "button_title": str(item.get("button_title") or "").strip(),
        "button_link": str(item.get("button_link") or "").strip(),
        "delay_after": max(0.0, min(float(item.get("delay_after") or 0), 30.0)),
    }


def enqueue_telegram_broadcast(
    base_dir: str | Path,
    *,
    source: str,
    items: list[dict[str, Any]],
    ttl_seconds: int = TELEGRAM_BROADCAST_EVENT_TTL_SECONDS,
) -> str | None:
    """Кладёт одну VK-рассылку в общую очередь Telegram.

    Файл-очередь позволяет не терять команду при кратком перезапуске Telegram-потока
    и не требует вызывать asyncio из VK Long Poll потока.
    """
    clean_items = [_clean_broadcast_item(item) for item in items if isinstance(item, dict)]
    clean_items = [
        item for item in clean_items
        if item["text"].strip() or item["photo_refs"].strip() or item["photo_files"]
    ]
    if not clean_items:
        return None

    path = Path(base_dir).resolve() / TELEGRAM_BROADCAST_QUEUE_FILE
    ensure_json_file(path, {"events": []})
    now_ts = time.time()
    event_id = uuid4().hex
    event = {
        "id": event_id,
        "source": str(source or "vk"),
        "status": "pending",
        "created_ts": now_ts,
        "created_at": _now_msk_str(),
        "expires_ts": now_ts + max(60, int(ttl_seconds or TELEGRAM_BROADCAST_EVENT_TTL_SECONDS)),
        "items": clean_items,
        "attempts": 0,
    }
    with TELEGRAM_BROADCAST_LOCK:
        data = read_json(path, {"events": []})
        if not isinstance(data, dict):
            data = {"events": []}
        events = data.get("events")
        if not isinstance(events, list):
            events = []
        # Завершённые события храним недолго, чтобы файл не рос бесконечно.
        events = [
            old for old in events
            if isinstance(old, dict)
            and not (
                old.get("status") in {"done", "expired"}
                and now_ts - float(old.get("finished_ts") or old.get("created_ts") or 0) > 86400
            )
        ]
        events.append(event)
        data["events"] = events[-500:]
        write_json_atomic(path, data)
    LOGGER.info("VK -> Telegram: поставлена в очередь рассылка %s (%s), сообщений: %s", event_id, source, len(clean_items))
    return event_id


def _split_csv_ints(value: str | None) -> set[int]:
    result: set[int] = set()
    for part in str(value or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            LOGGER.warning("Некорректный Telegram ID в настройках: %s", part)
    return result


def _split_csv_words(value: str | None) -> set[str]:
    return {
        part.strip().lstrip("@").casefold()
        for part in str(value or "").replace(";", ",").split(",")
        if part.strip()
    }


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_text(value: Any) -> str:
    text = str(value or "").casefold().replace("ё", "е")
    return " ".join(re.findall(r"[a-zа-я0-9]+", text))


def _now_msk_str() -> str:
    return datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")


def _format_application_time(item: dict[str, Any]) -> str:
    ready = str(item.get("source_time") or "").strip()
    if ready:
        return ready
    timestamp = item.get("source_ts") or item.get("created_ts") or time.time()
    try:
        dt = datetime.fromtimestamp(float(timestamp), tz=MSK)
    except (TypeError, ValueError, OSError):
        dt = datetime.now(MSK)
    return dt.strftime("%d.%m.%Y %H:%M МСК")


def _parse_vk_mention(value: Any) -> tuple[str, str | None]:
    raw = str(value or "").strip()
    match = re.fullmatch(r"\[(id|club)(-?\d+)\|([^\]]+)\]", raw)
    if not match:
        return raw or "Пользователь VK", None
    kind, ident, label = match.groups()
    if kind == "id":
        return label, f"https://vk.com/id{ident}"
    clean_id = ident.lstrip("-")
    return label, f"https://vk.com/club{clean_id}"


class TelegramCompanion:
    """Telegram-часть бота, работающая с теми же JSON-файлами, что и VK."""

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir).resolve()
        load_dotenv(self.base_dir / ".env")

        self.token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
        self.admin_ids = _split_csv_ints(os.getenv("TELEGRAM_ADMIN_IDS"))
        self.admin_usernames = _split_csv_words(
            os.getenv("TELEGRAM_ADMIN_USERNAMES") or "makswwy"
        )
        configured_admin_chat = _safe_int(os.getenv("TELEGRAM_ADMIN_CHAT_ID"))
        self.configured_admin_chat_ids = {configured_admin_chat} if configured_admin_chat else set()
        self.public_chat_id = _safe_int(os.getenv("TELEGRAM_PUBLIC_CHAT_ID"))
        self.configured_broadcast_chat_ids = _split_csv_ints(os.getenv("TELEGRAM_BROADCAST_CHAT_IDS"))
        self.hr_username = str(os.getenv("HR_TELEGRAM_USERNAME") or "makswwy").strip().lstrip("@")

        # Сетевые настройки Telegram. Увеличенные таймауты помогают на медленном
        # хостинге, а TELEGRAM_PROXY_URL позволяет работать там, где прямой
        # доступ к api.telegram.org ограничен.
        self.telegram_proxy_url = str(os.getenv("TELEGRAM_PROXY_URL") or "").strip() or None
        self.telegram_connect_timeout = max(5.0, _safe_float(os.getenv("TELEGRAM_CONNECT_TIMEOUT"), 30.0))
        self.telegram_read_timeout = max(10.0, _safe_float(os.getenv("TELEGRAM_READ_TIMEOUT"), 45.0))
        self.telegram_write_timeout = max(10.0, _safe_float(os.getenv("TELEGRAM_WRITE_TIMEOUT"), 45.0))
        self.telegram_media_write_timeout = max(20.0, _safe_float(os.getenv("TELEGRAM_MEDIA_WRITE_TIMEOUT"), 120.0))
        self.telegram_pool_timeout = max(5.0, _safe_float(os.getenv("TELEGRAM_POOL_TIMEOUT"), 30.0))

        self.data_path = self.base_dir / "data.json"
        self.pm_jobs_path = self.base_dir / "pm_jobs.json"
        self.applications_path = self.base_dir / "applications.json"
        self.ads_path = self.base_dir / "ads.json"
        self.orders_path = self.base_dir / "orders.json"
        self.telegram_users_path = self.base_dir / "telegram_users.json"
        self.broadcast_queue_path = self.base_dir / TELEGRAM_BROADCAST_QUEUE_FILE

        ensure_json_file(self.pm_jobs_path, {"next_id": 1, "items": []})
        ensure_json_file(self.applications_path, {"next_id": 1, "items": []})
        ensure_json_file(
            self.telegram_users_path,
            {"users": {}, "admin_chat_ids": [], "broadcast_chat_ids": [], "broadcast_disabled_chat_ids": [], "chats": {}, "created_at": _now_msk_str()},
        )
        ensure_json_file(self.broadcast_queue_path, {"events": []})

        self._vk_api: Any | None = None
        self._application: Application | None = None

    # ---------------- Общая база ----------------
    def _load_pm_jobs(self) -> dict[str, Any]:
        data = read_json(self.pm_jobs_path, {"next_id": 1, "items": []})
        if not isinstance(data, dict):
            return {"next_id": 1, "items": []}
        if not isinstance(data.get("items"), list):
            data["items"] = []
        return data

    def _active_pm_jobs(self) -> list[dict[str, Any]]:
        with PM_JOBS_LOCK:
            jobs = [
                dict(item)
                for item in self._load_pm_jobs().get("items", [])
                if item.get("active", True)
            ]
        jobs.sort(key=lambda item: _safe_int(item.get("id")))
        return jobs

    def _get_pm_job(self, job_id: int) -> dict[str, Any] | None:
        with PM_JOBS_LOCK:
            for item in self._load_pm_jobs().get("items", []):
                if _safe_int(item.get("id")) != job_id:
                    continue
                if not item.get("active", True):
                    return None
                return dict(item)
        return None

    def _load_applications(self) -> dict[str, Any]:
        data = read_json(self.applications_path, {"next_id": 1, "items": []})
        if not isinstance(data, dict):
            return {"next_id": 1, "items": []}
        if not isinstance(data.get("items"), list):
            data["items"] = []
        return data

    def _save_applications(self, data: dict[str, Any]) -> None:
        write_json_atomic(self.applications_path, data)

    def _load_telegram_users(self) -> dict[str, Any]:
        default = {
            "users": {},
            "admin_chat_ids": [],
            "broadcast_chat_ids": [],
            "broadcast_disabled_chat_ids": [],
            "chats": {},
            "created_at": _now_msk_str(),
        }
        data = read_json(self.telegram_users_path, default)
        if not isinstance(data, dict):
            data = dict(default)
        if not isinstance(data.get("users"), dict):
            data["users"] = {}
        if not isinstance(data.get("admin_chat_ids"), list):
            data["admin_chat_ids"] = []
        if not isinstance(data.get("broadcast_chat_ids"), list):
            data["broadcast_chat_ids"] = []
        if not isinstance(data.get("broadcast_disabled_chat_ids"), list):
            data["broadcast_disabled_chat_ids"] = []
        if not isinstance(data.get("chats"), dict):
            data["chats"] = {}
        return data

    def _save_telegram_users(self, data: dict[str, Any]) -> None:
        write_json_atomic(self.telegram_users_path, data)

    def _is_admin_user(self, user: Any) -> bool:
        if user is None:
            return False
        if int(user.id) in self.admin_ids:
            return True
        username = str(getattr(user, "username", "") or "").casefold()
        return bool(username and username in self.admin_usernames)

    def _set_broadcast_chat(self, chat: Any, enabled: bool, *, manual: bool = False) -> None:
        if chat is None or str(getattr(chat, "type", "")) == "private":
            return
        chat_id = _safe_int(getattr(chat, "id", 0))
        if not chat_id:
            return
        with TELEGRAM_USERS_LOCK:
            data = self._load_telegram_users()
            ids = {_safe_int(value) for value in data.get("broadcast_chat_ids", [])}
            disabled_ids = {_safe_int(value) for value in data.get("broadcast_disabled_chat_ids", [])}
            ids.discard(0)
            disabled_ids.discard(0)
            if enabled:
                if manual:
                    disabled_ids.discard(chat_id)
                if chat_id not in disabled_ids:
                    ids.add(chat_id)
            else:
                ids.discard(chat_id)
                if manual:
                    disabled_ids.add(chat_id)
            data["broadcast_chat_ids"] = sorted(ids)
            data["broadcast_disabled_chat_ids"] = sorted(disabled_ids)
            chats = data.setdefault("chats", {})
            chats[str(chat_id)] = {
                "id": chat_id,
                "type": str(getattr(chat, "type", "") or ""),
                "title": str(getattr(chat, "title", "") or ""),
                "username": str(getattr(chat, "username", "") or ""),
                "broadcast_enabled": chat_id in ids,
                "manually_disabled": chat_id in disabled_ids,
                "updated_at": _now_msk_str(),
            }
            self._save_telegram_users(data)

    def _remember_chat(self, update: Update) -> None:
        chat = update.effective_chat
        if chat is None or str(chat.type) == "private":
            return
        # Любой реально полученный update подтверждает, что чат доступен боту.
        self._set_broadcast_chat(chat, True, manual=False)

    def _broadcast_chat_ids(self) -> set[int]:
        """Возвращает все автоматически обнаруженные чаты для рассылки.

        Отдельная команда подключения не нужна: чат попадает в реестр после любого
        update от Telegram (включая добавление бота, сообщение, команду или изменение
        статуса). Чаты, вручную отключённые через /unsyncchat, сюда не попадают.
        """
        result = set(self.configured_broadcast_chat_ids)
        with TELEGRAM_USERS_LOCK:
            data = self._load_telegram_users()
            disabled_ids = {
                _safe_int(value)
                for value in data.get("broadcast_disabled_chat_ids", [])
                if _safe_int(value)
            }
            result.update(_safe_int(value) for value in data.get("broadcast_chat_ids", []))

            # Миграция/страховка: пробуем также все когда-либо замеченные неперсональные
            # чаты. Успешные останутся активными, недоступные будут удалены при отправке.
            for raw_chat_id, info in data.get("chats", {}).items():
                if not isinstance(info, dict):
                    continue
                chat_id = _safe_int(info.get("id") or raw_chat_id)
                chat_type = str(info.get("type") or "").lower()
                if chat_id and chat_type != "private" and chat_id not in disabled_ids:
                    result.add(chat_id)

            result.difference_update(disabled_ids)
        if self.public_chat_id:
            result.add(self.public_chat_id)
        result.discard(0)
        # Админские чаты используются для управления/уведомлений, рекламу туда не дублируем.
        result.difference_update(self._admin_chat_ids())
        return result

    def _mark_broadcast_delivery(self, chat_id: int, *, success: bool, error: str = "") -> None:
        """Сохраняет результат пробной/обычной отправки в реестр чатов."""
        chat_id = _safe_int(chat_id)
        if not chat_id:
            return
        with TELEGRAM_USERS_LOCK:
            data = self._load_telegram_users()
            ids = {_safe_int(value) for value in data.get("broadcast_chat_ids", []) if _safe_int(value)}
            chats = data.setdefault("chats", {})
            info = chats.setdefault(str(chat_id), {"id": chat_id, "type": "unknown"})
            if success:
                ids.add(chat_id)
                info["broadcast_enabled"] = True
                info["last_success_at"] = _now_msk_str()
                info.pop("last_error", None)
            else:
                ids.discard(chat_id)
                info["broadcast_enabled"] = False
                info["last_error"] = str(error or "")[:500]
                info["last_error_at"] = _now_msk_str()
            info["updated_at"] = _now_msk_str()
            data["broadcast_chat_ids"] = sorted(ids)
            self._save_telegram_users(data)

    def _remember_user(self, update: Update) -> None:
        self._remember_chat(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        with TELEGRAM_USERS_LOCK:
            data = self._load_telegram_users()
            data["users"][str(user.id)] = {
                "id": int(user.id),
                "chat_id": int(chat.id),
                "username": str(user.username or ""),
                "first_name": str(user.first_name or ""),
                "last_name": str(user.last_name or ""),
                "language_code": str(user.language_code or ""),
                "last_seen_at": _now_msk_str(),
            }
            if self._is_admin_user(user) and chat.type == "private":
                ids = {_safe_int(value) for value in data.get("admin_chat_ids", [])}
                ids.discard(0)
                ids.add(int(chat.id))
                data["admin_chat_ids"] = sorted(ids)
            self._save_telegram_users(data)

    def _admin_chat_ids(self) -> set[int]:
        result = set(self.configured_admin_chat_ids)
        with TELEGRAM_USERS_LOCK:
            data = self._load_telegram_users()
            result.update(_safe_int(value) for value in data.get("admin_chat_ids", []))
        result.discard(0)
        return result

    # ---------------- VK-мост ----------------
    def _vk(self) -> Any:
        if self._vk_api is None:
            if not str(group_token or "").strip():
                raise RuntimeError("VK group_token не настроен")
            self._vk_api = vk_api.VkApi(token=group_token).get_api()
        return self._vk_api

    def _vk_admin_chat(self) -> int:
        data = read_json(self.data_path, {})
        return _safe_int(data.get("admin_chat")) if isinstance(data, dict) else 0

    def _vk_public_chat(self) -> int:
        data = read_json(self.data_path, {})
        return _safe_int(data.get("applications_chat")) if isinstance(data, dict) else 0

    def _send_vk_message(self, peer_id: int, text: str) -> int:
        if not peer_id:
            raise RuntimeError("VK peer_id не настроен")
        result = self._vk().messages.send(
            peer_id=int(peer_id),
            random_id=random.randint(1, 2_147_483_647),
            message=str(text),
        )
        return _safe_int(result)

    def _resolve_vk_photo_urls(self, attachment_value: Any) -> list[str]:
        refs = []
        for raw in str(attachment_value or "").split(","):
            value = raw.strip()
            if re.fullmatch(r"photo-?\d+_\d+(?:_[A-Za-z0-9_-]+)?", value):
                refs.append(value)
        if not refs:
            return []
        try:
            photos = self._vk().photos.getById(photos=",".join(refs))
        except Exception as exc:
            LOGGER.warning("Не удалось получить фото VK для Telegram: %s", exc)
            return []
        urls: list[str] = []
        for photo in photos or []:
            sizes = photo.get("sizes") or []
            best = max(
                sizes,
                key=lambda size: _safe_int(size.get("width")) * _safe_int(size.get("height")),
                default=None,
            )
            url = str((best or {}).get("url") or "").strip()
            if url:
                urls.append(url)
        return urls

    # ---------------- Каталог работ ----------------
    def _jobs_page(self, page: int) -> tuple[list[dict[str, Any]], int, int]:
        jobs = self._active_pm_jobs()
        page_count = max(1, (len(jobs) + PM_JOBS_PAGE_SIZE - 1) // PM_JOBS_PAGE_SIZE)
        safe_page = max(0, min(int(page or 0), page_count - 1))
        start = safe_page * PM_JOBS_PAGE_SIZE
        return jobs[start:start + PM_JOBS_PAGE_SIZE], safe_page, page_count

    def _jobs_keyboard(self, page: int) -> InlineKeyboardMarkup | None:
        items, safe_page, page_count = self._jobs_page(page)
        if not items:
            return None
        rows: list[list[InlineKeyboardButton]] = []
        for item in items:
            title = str(item.get("title") or f"Работа №{item.get('id')}").strip()
            if len(title) > 55:
                title = title[:52].rstrip() + "..."
            rows.append([
                InlineKeyboardButton(
                    title,
                    callback_data=f"job:{_safe_int(item.get('id'))}:{safe_page}",
                )
            ])
        nav: list[InlineKeyboardButton] = []
        if safe_page > 0:
            nav.append(InlineKeyboardButton("← Назад", callback_data=f"jobs:{safe_page - 1}"))
        if safe_page < page_count - 1:
            nav.append(InlineKeyboardButton("Далее →", callback_data=f"jobs:{safe_page + 1}"))
        if nav:
            rows.append(nav)
        return InlineKeyboardMarkup(rows)

    def _job_keyboard(self, job_id: int, page: int) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "Интересно, вызвать Менеджера",
                callback_data=f"interest:{job_id}:{page}",
            )],
            [InlineKeyboardButton("← Назад", callback_data=f"jobs:{page}")],
        ])

    async def _send_text_chunks(
        self,
        chat_id: int,
        text: str,
        *,
        reply_markup: InlineKeyboardMarkup | None = None,
        parse_mode: str | None = None,
    ) -> None:
        clean = str(text or "")
        chunks: list[str] = []
        while clean:
            if len(clean) <= 3900:
                chunks.append(clean)
                break
            split_at = clean.rfind("\n", 0, 3900)
            if split_at < 1000:
                split_at = 3900
            chunks.append(clean[:split_at])
            clean = clean[split_at:].lstrip("\n")
        if not chunks:
            chunks = [""]
        for index, chunk in enumerate(chunks):
            await self._application.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                reply_markup=reply_markup if index == len(chunks) - 1 else None,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )

    async def _show_jobs(self, update: Update, page: int = 0) -> None:
        self._remember_user(update)
        chat = update.effective_chat
        if chat is None:
            return
        items, safe_page, page_count = self._jobs_page(page)
        if not items:
            await self._application.bot.send_message(
                chat_id=chat.id,
                text="Сейчас актуальных работ нет. Попробуйте немного позже.",
            )
            return
        await self._application.bot.send_message(
            chat_id=chat.id,
            text=(
                "Доступные работы\n\n"
                "Выберите интересующий вариант кнопкой ниже.\n"
                f"Страница {safe_page + 1} из {page_count}"
            ),
            reply_markup=self._jobs_keyboard(safe_page),
        )

    async def _show_job(self, update: Update, job_id: int, page: int) -> None:
        self._remember_user(update)
        chat = update.effective_chat
        if chat is None:
            return
        job = self._get_pm_job(job_id)
        if not job:
            await self._application.bot.send_message(
                chat_id=chat.id,
                text="Эта работа уже неактуальна. Показываю актуальный список.",
            )
            await self._show_jobs(update, page)
            return

        title = str(job.get("title") or f"Работа №{job_id}").strip()
        description = str(job.get("description") or "Описание не указано.").strip()
        keyboard = self._job_keyboard(job_id, page)
        photo_urls = await asyncio.to_thread(
            self._resolve_vk_photo_urls,
            job.get("attachment"),
        )

        # Подпись Telegram ограничена сильнее обычного сообщения, поэтому
        # длинное описание отправляется текстом, а фото — отдельным сообщением.
        if photo_urls:
            try:
                await self._application.bot.send_photo(
                    chat_id=chat.id,
                    photo=photo_urls[0],
                    caption=html.escape(title)[:1000],
                    parse_mode=ParseMode.HTML,
                )
                for extra_url in photo_urls[1:10]:
                    await self._application.bot.send_photo(chat_id=chat.id, photo=extra_url)
            except Exception as exc:
                LOGGER.warning("Не удалось отправить фото работы в Telegram: %s", exc)

        await self._send_text_chunks(
            chat.id,
            f"{title}\n\n{description}",
            reply_markup=keyboard,
        )

    # ---------------- Уведомления менеджеру ----------------
    def _tg_user_html(self, user: Any) -> str:
        label = " ".join(
            part for part in [str(user.first_name or ""), str(user.last_name or "")] if part
        ).strip() or str(user.username or "Пользователь Telegram")
        return f'<a href="tg://user?id={int(user.id)}">{html.escape(label)}</a>'

    async def _notify_interest(self, update: Update, job: dict[str, Any]) -> None:
        user = update.effective_user
        if user is None:
            return
        title = str(job.get("title") or f"Работа №{job.get('id')}").strip()
        company_name = str(job.get("company_name") or "Не указана").strip()
        username = str(user.username or "").strip()
        tg_profile = f"https://t.me/{username}" if username else f"Telegram ID: {user.id}"
        display_name = " ".join(
            part for part in [str(user.first_name or ""), str(user.last_name or "")] if part
        ).strip() or "Пользователь Telegram"

        vk_text = (
            "Пользователь заинтересовался работой из личных сообщений Telegram:\n"
            f"Имя и фамилия: {display_name}\n"
            + (f"Username: @{username}\n" if username else "")
            + f"Профиль: {tg_profile}\n"
            f"Telegram ID: {user.id}\n"
            f"Работа: {title}\n"
            f"Компания: {company_name}\n\n"
            f"Срочно ответить HR-менеджеру @{self.hr_username}"
        )
        vk_admin_chat = self._vk_admin_chat()
        if vk_admin_chat:
            try:
                await asyncio.to_thread(self._send_vk_message, vk_admin_chat, vk_text)
            except Exception as exc:
                LOGGER.exception("Не удалось уведомить VK-админ-чат: %s", exc)

        tg_text = (
            "Пользователь заинтересовался работой из личных сообщений Telegram:\n"
            f"Имя и фамилия: <b>{html.escape(display_name)}</b>\n"
            f"Профиль: {self._tg_user_html(user)}\n"
            f"Работа: <b>{html.escape(title)}</b>\n"
            f"Компания: <b>{html.escape(company_name)}</b>\n\n"
            f"Срочно ответить HR-менеджеру @{html.escape(self.hr_username)}"
        )
        for admin_chat_id in self._admin_chat_ids():
            try:
                await self._application.bot.send_message(
                    chat_id=admin_chat_id,
                    text=tg_text,
                    parse_mode=ParseMode.HTML,
                )
            except (Forbidden, BadRequest) as exc:
                LOGGER.warning("Не удалось написать Telegram-администратору %s: %s", admin_chat_id, exc)

    # ---------------- Заявки и заказы для Telegram-админа ----------------
    def _render_application_admin_html(self, item: dict[str, Any]) -> str:
        author_label, author_url = _parse_vk_mention(item.get("author_mention"))
        author_html = html.escape(author_label)
        if author_url:
            author_html = f'<a href="{html.escape(author_url)}">{author_html}</a>'
        text = html.escape(str(item.get("text") or "").strip())
        city = html.escape(str(item.get("city") or "Не определён"))
        chat_title = html.escape(str(item.get("source_chat_title") or "Не удалось определить"))
        return (
            f"<b>Новая заявка №{_safe_int(item.get('id'))}</b>\n\n"
            f"Заявка от — {author_html}\n"
            f"Беседа: {chat_title}\n"
            f"ID беседы: <code>{html.escape(str(item.get('source_peer_id') or ''))}</code>\n"
            f"Время: {html.escape(_format_application_time(item))}\n\n"
            f"{text}\n\n"
            f"Город: <b>{city}</b>"
        )

    def _render_application_public_text(self, item: dict[str, Any]) -> str:
        return (
            f"{str(item.get('text') or '').strip()}\n\n"
            f"Работодатель: {str(item.get('author_mention') or 'Пользователь VK')}\n"
            f"Город: {str(item.get('city') or 'не определён')}\n"
            f"Время: {_format_application_time(item)}"
        )

    async def _show_pending_applications(self, update: Update) -> None:
        self._remember_user(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        if not self._is_admin_user(user):
            await self._application.bot.send_message(chat.id, "Недостаточно прав.")
            return
        with APPLICATIONS_LOCK:
            data = self._load_applications()
            pending = [dict(item) for item in data.get("items", []) if item.get("status") == "pending"]
        if not pending:
            await self._application.bot.send_message(chat.id, "Новых заявок на проверке нет.")
            return
        await self._application.bot.send_message(
            chat.id,
            f"Заявок на проверке: {len(pending)}. Показываю последние 10.",
        )
        for item in pending[-10:]:
            app_id = _safe_int(item.get("id"))
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"app:approve:{app_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"app:reject:{app_id}"),
            ]])
            await self._application.bot.send_message(
                chat.id,
                self._render_application_admin_html(item),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

    async def _process_application_action(self, update: Update, action: str, application_id: int) -> None:
        query = update.callback_query
        user = update.effective_user
        if query is None or user is None:
            return
        if not self._is_admin_user(user):
            await query.answer("Недостаточно прав.", show_alert=True)
            return

        with APPLICATIONS_LOCK:
            data = self._load_applications()
            item = next(
                (entry for entry in data.get("items", []) if _safe_int(entry.get("id")) == application_id),
                None,
            )
            if not item or item.get("status") != "pending":
                await query.answer("Заявка уже обработана.", show_alert=True)
                return

            if action == "reject":
                item["status"] = "rejected"
                item["processed_at"] = _now_msk_str()
                item["processed_by_telegram"] = int(user.id)
                self._save_applications(data)
                await query.answer("Заявка отклонена.")
                try:
                    await query.edit_message_reply_markup(reply_markup=None)
                except BadRequest:
                    pass
                return

            publish_chat = self._vk_public_chat()
            if not publish_chat:
                await query.answer("Сначала установите чат командой .устзаявки в VK.", show_alert=True)
                return
            public_text = self._render_application_public_text(item)

        # VK-вызов делается вне блокировки, чтобы не задерживать VK-поток.
        try:
            published_id = await asyncio.to_thread(self._send_vk_message, publish_chat, public_text)
        except Exception as exc:
            LOGGER.exception("Не удалось опубликовать заявку из Telegram: %s", exc)
            await query.answer("Не удалось опубликовать заявку в VK.", show_alert=True)
            return

        with APPLICATIONS_LOCK:
            data = self._load_applications()
            item = next(
                (entry for entry in data.get("items", []) if _safe_int(entry.get("id")) == application_id),
                None,
            )
            if not item or item.get("status") != "pending":
                await query.answer("Заявка уже обработана другим администратором.", show_alert=True)
                return
            now_ts = time.time()
            item["status"] = "published"
            item["processed_at"] = _now_msk_str()
            item["processed_by_telegram"] = int(user.id)
            item["published_message_id"] = int(published_id)
            item["published_peer_id"] = int(publish_chat)
            item["published_ts"] = now_ts
            item["expires_ts"] = now_ts + APPLICATION_LIFETIME_SECONDS
            self._save_applications(data)

        await query.answer("Опубликовано на 6 часов.")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass

    def _active_orders_summary(self) -> str:
        orders = read_json(self.orders_path, {})
        ads = read_json(self.ads_path, {})
        if not isinstance(orders, dict):
            orders = {}
        if not isinstance(ads, dict):
            ads = {}
        rows: list[str] = []
        for key, ad in ads.items():
            if not isinstance(ad, dict) or ad.get("status") != "active":
                continue
            order_id = str(ad.get("order_id") or "")
            order = orders.get(order_id, {}) if order_id else {}
            code = str(ad.get("order_code") or order.get("order_code") or key).strip()
            state = "приостановлен" if ad.get("broadcast_paused") else "в рассылке"
            expires = str(ad.get("expires_at") or order.get("expires_at") or "не указано")
            rows.append(f"• {code} — {state}, до {expires}")
        if not rows:
            return "Активных заказов нет."
        return "Активные заказы:\n" + "\n".join(rows[:30])

    async def _show_status(self, update: Update) -> None:
        self._remember_user(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        if not self._is_admin_user(user):
            await self._application.bot.send_message(chat.id, "Недостаточно прав.")
            return
        with APPLICATIONS_LOCK:
            apps = self._load_applications().get("items", [])
            pending = sum(1 for item in apps if item.get("status") == "pending")
            published = sum(1 for item in apps if item.get("status") == "published")
        jobs = len(self._active_pm_jobs())
        with TELEGRAM_BROADCAST_LOCK:
            queue_events = self._load_broadcast_queue().get("events", [])
            queued = sum(1 for item in queue_events if isinstance(item, dict) and item.get("status") in {"pending", "processing"})
        await self._application.bot.send_message(
            chat.id,
            f"Общая база VK + Telegram\n\n"
            f"Работ в каталоге: {jobs}\n"
            f"Заявок на проверке: {pending}\n"
            f"Опубликованных заявок: {published}\n"
            f"Telegram-чатов общей рассылки: {len(self._broadcast_chat_ids())}\n"
            f"Рассылок в очереди: {queued}\n"
            f"VK чат публикации: {self._vk_public_chat() or 'не установлен'}\n"
            f"Telegram чат публикации: {self.public_chat_id or 'не установлен'}",
        )

    # ---------------- Синхронизация публикаций VK -> Telegram ----------------
    async def _published_sync_loop(self) -> None:
        while True:
            try:
                if self.public_chat_id:
                    await self._sync_published_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.exception("Ошибка синхронизации опубликованных заявок в Telegram: %s", exc)
            await asyncio.sleep(15)

    async def _sync_published_once(self) -> None:
        now_ts = time.time()
        with APPLICATIONS_LOCK:
            snapshot = [dict(item) for item in self._load_applications().get("items", [])]

        for item in snapshot:
            if item.get("status") != "published":
                continue
            app_id = _safe_int(item.get("id"))
            expires_ts = float(item.get("expires_ts", 0) or 0)
            if not expires_ts:
                published_ts = float(item.get("published_ts", 0) or 0)
                expires_ts = published_ts + APPLICATION_LIFETIME_SECONDS if published_ts else 0
            tg_message_id = _safe_int(item.get("telegram_published_message_id"))
            tg_chat_id = _safe_int(item.get("telegram_published_chat_id")) or self.public_chat_id

            if expires_ts and expires_ts <= now_ts:
                if tg_message_id and tg_chat_id and not item.get("telegram_deleted_at"):
                    try:
                        await self._application.bot.delete_message(tg_chat_id, tg_message_id)
                    except (BadRequest, Forbidden):
                        pass
                    with APPLICATIONS_LOCK:
                        data = self._load_applications()
                        current = next(
                            (entry for entry in data.get("items", []) if _safe_int(entry.get("id")) == app_id),
                            None,
                        )
                        if current:
                            current["telegram_deleted_at"] = _now_msk_str()
                            self._save_applications(data)
                continue

            if tg_message_id:
                continue

            text = self._render_application_public_text(item)
            sent = await self._application.bot.send_message(
                chat_id=self.public_chat_id,
                text=text,
                disable_web_page_preview=True,
            )
            with APPLICATIONS_LOCK:
                data = self._load_applications()
                current = next(
                    (entry for entry in data.get("items", []) if _safe_int(entry.get("id")) == app_id),
                    None,
                )
                if current and not current.get("telegram_published_message_id"):
                    current["telegram_published_chat_id"] = int(self.public_chat_id)
                    current["telegram_published_message_id"] = int(sent.message_id)
                    current["telegram_published_at"] = _now_msk_str()
                    self._save_applications(data)

    # ---------------- Полная синхронизация VK-рассылок -> Telegram ----------------
    def _load_broadcast_queue(self) -> dict[str, Any]:
        data = read_json(self.broadcast_queue_path, {"events": []})
        if not isinstance(data, dict):
            data = {"events": []}
        if not isinstance(data.get("events"), list):
            data["events"] = []
        return data

    def _claim_broadcast_event(self) -> dict[str, Any] | None:
        now_ts = time.time()
        with TELEGRAM_BROADCAST_LOCK:
            data = self._load_broadcast_queue()
            selected = None
            changed = False
            for event in data.get("events", []):
                if not isinstance(event, dict):
                    continue
                status = str(event.get("status") or "pending")
                expires_ts = float(event.get("expires_ts") or 0)
                if expires_ts and expires_ts <= now_ts and status not in {"done", "expired"}:
                    event["status"] = "expired"
                    event["finished_ts"] = now_ts
                    changed = True
                    continue
                retry_after = float(event.get("retry_after_ts") or 0)
                if retry_after and retry_after > now_ts:
                    continue
                processing_since = float(event.get("processing_since_ts") or 0)
                stale_processing = status == "processing" and now_ts - processing_since > 300
                if status == "pending" or stale_processing:
                    event["status"] = "processing"
                    event["processing_since_ts"] = now_ts
                    event["attempts"] = _safe_int(event.get("attempts")) + 1
                    selected = dict(event)
                    changed = True
                    break
            if changed:
                self._save_broadcast_queue(data)
            return selected

    def _save_broadcast_queue(self, data: dict[str, Any]) -> None:
        write_json_atomic(self.broadcast_queue_path, data)

    def _finish_broadcast_event(
        self,
        event_id: str,
        *,
        status: str,
        sent_chats: int = 0,
        failed_chats: int = 0,
        error: str = "",
        retry_after: float = 0,
    ) -> None:
        with TELEGRAM_BROADCAST_LOCK:
            data = self._load_broadcast_queue()
            current = next(
                (event for event in data.get("events", []) if str(event.get("id")) == str(event_id)),
                None,
            )
            if not current:
                return
            current["status"] = status
            current["sent_chats"] = int(sent_chats)
            current["failed_chats"] = int(failed_chats)
            current["last_error"] = str(error or "")[:1000]
            current.pop("processing_since_ts", None)
            if retry_after:
                current["retry_after_ts"] = time.time() + float(retry_after)
            else:
                current.pop("retry_after_ts", None)
            if status in {"done", "expired", "failed"}:
                current["finished_ts"] = time.time()
                current["finished_at"] = _now_msk_str()
            self._save_broadcast_queue(data)

    def _broadcast_keyboard(self, item: dict[str, Any]) -> InlineKeyboardMarkup | None:
        title = str(item.get("button_title") or "").strip()
        link = str(item.get("button_link") or "").strip()
        if not title or not link:
            return None
        if "://" not in link:
            link = "https://" + link
        return InlineKeyboardMarkup([[InlineKeyboardButton(title[:64], url=link)]])

    async def _resolve_broadcast_media(self, item: dict[str, Any]) -> list[tuple[str, Any]]:
        refs = str(item.get("photo_refs") or "").strip()
        urls = await asyncio.to_thread(self._resolve_vk_photo_urls, refs) if refs else []
        if urls:
            return [("url", url) for url in urls[:10]]
        result: list[tuple[str, Any]] = []
        raw_files = item.get("photo_files") or []
        if not isinstance(raw_files, list):
            raw_files = [raw_files]
        for raw in raw_files:
            value = str(raw or "").strip()
            if not value or value.startswith(("photo", "video", "doc")):
                continue
            path = Path(value)
            if not path.is_absolute():
                path = self.base_dir / path
            if path.exists() and path.is_file():
                result.append(("file", path))
            if len(result) >= 10:
                break
        return result

    async def _send_photo_source(
        self,
        chat_id: int,
        source: tuple[str, Any],
        *,
        caption: str | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        kind, value = source
        if kind == "file":
            with Path(value).open("rb") as handle:
                await self._application.bot.send_photo(
                    chat_id=chat_id,
                    photo=handle,
                    caption=caption,
                    reply_markup=reply_markup,
                )
            return
        await self._application.bot.send_photo(
            chat_id=chat_id,
            photo=value,
            caption=caption,
            reply_markup=reply_markup,
        )

    async def _send_broadcast_item(self, chat_id: int, item: dict[str, Any]) -> None:
        text = str(item.get("text") or "").strip()
        keyboard = self._broadcast_keyboard(item)
        media = await self._resolve_broadcast_media(item)

        # Один снимок + короткий текст можно отправить одной компактной карточкой.
        if len(media) == 1 and len(text) <= 1000:
            await self._send_photo_source(
                chat_id,
                media[0],
                caption=text or None,
                reply_markup=keyboard,
            )
            return

        # Несколько фото отправляем все, затем полный текст и кнопку отдельным сообщением.
        for source in media:
            await self._send_photo_source(chat_id, source)
        if text or keyboard:
            await self._send_text_chunks(chat_id, text or "Открыть", reply_markup=keyboard)

    async def _deliver_broadcast_to_chat(self, chat_id: int, event: dict[str, Any]) -> None:
        for item in event.get("items", []):
            if not isinstance(item, dict):
                continue
            await self._send_broadcast_item(chat_id, item)
            delay = max(0.0, min(float(item.get("delay_after") or 0), 30.0))
            if delay:
                await asyncio.sleep(delay)

    async def _deliver_broadcast_event(self, event: dict[str, Any]) -> None:
        event_id = str(event.get("id") or "")
        targets = sorted(self._broadcast_chat_ids())
        if not targets:
            # Даём время Telegram прислать update о добавлении бота или активности чата.
            attempts = _safe_int(event.get("attempts"))
            if attempts < 20:
                self._finish_broadcast_event(event_id, status="pending", error="Нет зарегистрированных Telegram-чатов", retry_after=30)
            else:
                self._finish_broadcast_event(event_id, status="failed", error="Нет зарегистрированных Telegram-чатов")
            return

        semaphore = asyncio.Semaphore(6)
        sent = 0
        failed = 0

        async def worker(chat_id: int) -> tuple[int, bool, str]:
            async with semaphore:
                try:
                    await self._deliver_broadcast_to_chat(chat_id, event)
                    self._mark_broadcast_delivery(chat_id, success=True)
                    return chat_id, True, ""
                except RetryAfter as exc:
                    await asyncio.sleep(float(exc.retry_after) + 1)
                    try:
                        await self._deliver_broadcast_to_chat(chat_id, event)
                        self._mark_broadcast_delivery(chat_id, success=True)
                        return chat_id, True, ""
                    except (Forbidden, BadRequest) as retry_exc:
                        self._mark_broadcast_delivery(chat_id, success=False, error=str(retry_exc))
                        return chat_id, False, str(retry_exc)
                    except Exception as retry_exc:
                        # Временную сетевую ошибку сохраняем в логах, но чат не забываем.
                        return chat_id, False, str(retry_exc)
                except (Forbidden, BadRequest) as exc:
                    # Telegram подтвердил, что писать нельзя/чат не существует.
                    # Удаляем его из активной рассылки автоматически.
                    self._mark_broadcast_delivery(chat_id, success=False, error=str(exc))
                    return chat_id, False, str(exc)
                except Exception as exc:
                    # TimedOut/NetworkError могут быть временными — не отключаем чат.
                    return chat_id, False, str(exc)

        results = await asyncio.gather(*(worker(chat_id) for chat_id in targets))
        errors: list[str] = []
        for chat_id, ok, error in results:
            if ok:
                sent += 1
            else:
                failed += 1
                errors.append(f"{chat_id}: {error}")
        self._finish_broadcast_event(
            event_id,
            status="done" if sent else "failed",
            sent_chats=sent,
            failed_chats=failed,
            error="; ".join(errors)[:1000],
        )
        LOGGER.info(
            "VK -> Telegram: рассылка %s завершена. Успешно: %s, ошибок: %s",
            event_id, sent, failed,
        )

    async def _broadcast_queue_loop(self) -> None:
        while True:
            try:
                event = self._claim_broadcast_event()
                if event:
                    await self._deliver_broadcast_event(event)
                else:
                    await asyncio.sleep(2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.exception("Ошибка очереди VK -> Telegram: %s", exc)
                await asyncio.sleep(5)

    # ---------------- Telegram handlers ----------------
    async def observe_update_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_chat(update)

    async def my_chat_member_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        change = update.my_chat_member
        chat = update.effective_chat
        if change is None or chat is None:
            return
        status = str(getattr(change.new_chat_member, "status", "") or "")
        enabled = status in {"member", "administrator", "creator", "restricted"}
        self._set_broadcast_chat(chat, enabled, manual=False)
        LOGGER.info(
            "Telegram-чат %s (%s): синхронизация рассылки %s, статус бота=%s",
            chat.id, getattr(chat, "title", ""), "включена" if enabled else "отключена", status,
        )

    async def syncchat_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        if str(chat.type) == "private":
            await self._application.bot.send_message(chat.id, "Эту команду нужно выполнить в Telegram-группе, комментариях или канале.")
            return
        if not self._is_admin_user(user):
            await self._application.bot.send_message(chat.id, "Недостаточно прав для подключения чата к рассылке.")
            return
        self._set_broadcast_chat(chat, True, manual=True)
        await self._application.bot.send_message(
            chat.id,
            "Чат подключён к общей VK + Telegram рассылке. Команды .gzov, .рассылка и .отпрзаказ из VK будут дублироваться сюда полностью.",
        )

    async def unsyncchat_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        if str(chat.type) == "private":
            return
        if not self._is_admin_user(user):
            await self._application.bot.send_message(chat.id, "Недостаточно прав.")
            return
        self._set_broadcast_chat(chat, False, manual=True)
        await self._application.bot.send_message(chat.id, "Этот чат отключён от общей рассылки.")

    async def start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        chat = update.effective_chat
        if chat is None:
            return
        await self._application.bot.send_message(
            chat.id,
            "Здравствуйте! Напишите «Работа» или нажмите /work, чтобы открыть каталог.",
        )
        await self._show_jobs(update, 0)

    async def work_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        await self._show_jobs(update, 0)

    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            return
        normalized = _normalize_text(message.text)
        work_commands = {
            "работа", "работы", "работу", "вакансия", "вакансии",
            "посмотреть работу", "показать работу", "найти работу",
        }
        if normalized in work_commands or normalized.startswith(("работа ", "вакансия ", "вакансии ")):
            await self._show_jobs(update, 0)
            return
        await self._application.bot.send_message(
            chat.id,
            "Напишите «Работа», чтобы открыть список доступных работ.",
        )

    async def applications_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        await self._show_pending_applications(update)

    async def orders_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        if not self._is_admin_user(user):
            await self._application.bot.send_message(chat.id, "Недостаточно прав.")
            return
        await self._send_text_chunks(chat.id, self._active_orders_summary())

    async def status_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        await self._show_status(update)

    async def whoami_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return
        await self._application.bot.send_message(
            chat.id,
            f"Ваш Telegram ID: {user.id}\n"
            f"ID текущего чата: {chat.id}\n"
            f"Username: @{user.username}" if user.username else f"Ваш Telegram ID: {user.id}\nID текущего чата: {chat.id}\nUsername не установлен",
        )

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self._remember_user(update)
        query = update.callback_query
        if query is None:
            return
        data = str(query.data or "")
        parts = data.split(":")
        try:
            if parts[0] == "jobs" and len(parts) == 2:
                await query.answer()
                await self._show_jobs(update, _safe_int(parts[1]))
                return
            if parts[0] == "job" and len(parts) == 3:
                await query.answer()
                await self._show_job(update, _safe_int(parts[1]), _safe_int(parts[2]))
                return
            if parts[0] == "interest" and len(parts) == 3:
                job = self._get_pm_job(_safe_int(parts[1]))
                if not job:
                    await query.answer("Эта работа уже неактуальна.", show_alert=True)
                    return
                await query.answer("Менеджер уведомлён.")
                await self._notify_interest(update, job)
                await self._application.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=(
                        "Ваш интерес передан менеджеру. Ожидайте, скоро с вами свяжется "
                        f"HR-менеджер @{self.hr_username}."
                    ),
                    reply_markup=self._job_keyboard(_safe_int(parts[1]), _safe_int(parts[2])),
                )
                return
            if parts[0] == "app" and len(parts) == 3:
                await self._process_application_action(update, parts[1], _safe_int(parts[2]))
                return
        except Exception as exc:
            LOGGER.exception("Ошибка обработки Telegram callback %s: %s", data, exc)
            try:
                await query.answer("Произошла ошибка. Попробуйте ещё раз.", show_alert=True)
            except Exception:
                pass

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        error = context.error
        if isinstance(error, RetryAfter):
            LOGGER.warning("Telegram flood control, пауза %s", error.retry_after)
        elif isinstance(error, (TimedOut, NetworkError)):
            LOGGER.warning("Временная ошибка Telegram: %s", error)
        else:
            LOGGER.error(
                "Необработанная ошибка Telegram update=%r: %s",
                update,
                error,
                exc_info=(type(error), error, error.__traceback__) if error else None,
            )

    async def post_init(self, application: Application) -> None:
        self._application = application
        try:
            await application.bot.set_my_commands([
                BotCommand("start", "Открыть бота"),
                BotCommand("work", "Показать работы"),
                BotCommand("applications", "Заявки на проверке — админ"),
                BotCommand("orders", "Активные заказы — админ"),
                BotCommand("status", "Состояние общей базы — админ"),
                BotCommand("whoami", "Показать Telegram ID"),
                BotCommand("unsyncchat", "Отключить текущий Telegram-чат от VK-рассылки"),
            ])
            LOGGER.info("Telegram-команды зарегистрированы")
        except (TimedOut, NetworkError) as exc:
            # Отсутствие ответа на setMyCommands не должно останавливать весь бот.
            LOGGER.warning("Telegram временно недоступен при регистрации команд: %s", exc)

    def build_application(self) -> Application:
        request_kwargs: dict[str, Any] = {
            "connection_pool_size": 64,
            "connect_timeout": self.telegram_connect_timeout,
            "read_timeout": self.telegram_read_timeout,
            "write_timeout": self.telegram_write_timeout,
            "media_write_timeout": self.telegram_media_write_timeout,
            "pool_timeout": self.telegram_pool_timeout,
            "http_version": "1.1",
        }
        updates_request_kwargs: dict[str, Any] = {
            "connection_pool_size": 2,
            "connect_timeout": self.telegram_connect_timeout,
            # Long polling ждёт до 50 секунд, поэтому read_timeout должен быть больше.
            "read_timeout": max(70.0, self.telegram_read_timeout),
            "write_timeout": self.telegram_write_timeout,
            "pool_timeout": self.telegram_pool_timeout,
            "http_version": "1.1",
        }
        if self.telegram_proxy_url:
            request_kwargs["proxy"] = self.telegram_proxy_url
            updates_request_kwargs["proxy"] = self.telegram_proxy_url
            LOGGER.info("Telegram будет подключаться через TELEGRAM_PROXY_URL")

        application = (
            ApplicationBuilder()
            .token(self.token)
            .request(HTTPXRequest(**request_kwargs))
            .get_updates_request(HTTPXRequest(**updates_request_kwargs))
            .post_init(self.post_init)
            .build()
        )
        application.add_handler(TypeHandler(Update, self.observe_update_handler), group=-1)
        application.add_handler(ChatMemberHandler(self.my_chat_member_handler, ChatMemberHandler.MY_CHAT_MEMBER))
        application.add_handler(CommandHandler("syncchat", self.syncchat_handler))
        application.add_handler(CommandHandler("unsyncchat", self.unsyncchat_handler))
        application.add_handler(CommandHandler("start", self.start_handler))
        application.add_handler(CommandHandler("work", self.work_handler))
        application.add_handler(CommandHandler("jobs", self.work_handler))
        application.add_handler(CommandHandler("applications", self.applications_handler))
        application.add_handler(CommandHandler("orders", self.orders_handler))
        application.add_handler(CommandHandler("status", self.status_handler))
        application.add_handler(CommandHandler("whoami", self.whoami_handler))
        application.add_handler(CallbackQueryHandler(self.callback_handler))
        application.add_handler(
            MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, self.text_handler)
        )
        application.add_error_handler(self.error_handler)
        return application

    async def run(self) -> None:
        application = self.build_application()
        self._application = application
        initialized = False
        polling_started = False
        application_started = False
        try:
            LOGGER.info(
                "Подключение Telegram к api.telegram.org (connect_timeout=%ss, proxy=%s)",
                self.telegram_connect_timeout,
                "да" if self.telegram_proxy_url else "нет",
            )
            await application.initialize()
            initialized = True
            if application.post_init:
                await application.post_init(application)
            if application.updater is None:
                raise RuntimeError("Telegram Updater не создан")
            await application.updater.start_polling(
                drop_pending_updates=False,
                allowed_updates=Update.ALL_TYPES,
                timeout=50,
                bootstrap_retries=-1,
            )
            polling_started = True
            await application.start()
            application_started = True
            application.create_task(
                self._published_sync_loop(),
                name="vk-to-telegram-publications",
            )
            application.create_task(
                self._broadcast_queue_loop(),
                name="vk-to-telegram-broadcasts",
            )
            LOGGER.info("Telegram-бот запущен: общая база и полная синхронизация VK-рассылок активны")
            while True:
                await asyncio.sleep(3600)
        finally:
            if polling_started and application.updater is not None:
                try:
                    await application.updater.stop()
                except Exception:
                    LOGGER.exception("Ошибка остановки Telegram polling")
            if application_started:
                try:
                    await application.stop()
                except Exception:
                    LOGGER.exception("Ошибка остановки Telegram application")
            if initialized:
                try:
                    await application.shutdown()
                except Exception:
                    LOGGER.exception("Ошибка завершения Telegram application")


def start_telegram_companion(base_dir: str | Path) -> threading.Thread | None:
    """Запускает Telegram в отдельном потоке рядом с VK Long Poll.

    Токен намеренно берётся только из TELEGRAM_BOT_TOKEN, чтобы секреты не
    попадали в исходный код и архивы.
    """
    companion = TelegramCompanion(base_dir)
    if not companion.token:
        LOGGER.warning(
            "Telegram-бот не запущен: задайте TELEGRAM_BOT_TOKEN в .env или переменных окружения"
        )
        return None

    def supervisor() -> None:
        retry_delay = 5
        while True:
            try:
                asyncio.run(companion.run())
                retry_delay = 5
            except (TimedOut, NetworkError) as exc:
                LOGGER.warning(
                    "Нет соединения с Telegram API: %s. Следующая попытка через %s секунд. "
                    "VK-часть продолжает работать.",
                    exc,
                    retry_delay,
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 120)
            except Exception as exc:
                LOGGER.exception(
                    "Telegram-бот остановился из-за ошибки, перезапуск через %s секунд: %s",
                    retry_delay,
                    exc,
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 120)

    thread = threading.Thread(
        target=supervisor,
        name="telegram-bot-supervisor",
        daemon=True,
    )
    thread.start()
    return thread
