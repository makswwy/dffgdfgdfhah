from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from vk_api.keyboard import VkKeyboard, VkKeyboardColor

from config import CUSTOM_BUTTON_PRICE, MAIN_TEXT_RATES, MENTION_ALL_PRICE, PAYMENT_DETAILS, RATES, SUBSCRIPTION_CHANNEL_LINK, group_id
from menu import generate_main_menu, generate_main_text_rates_menu, generate_rates_menu
from services.ad_safety import AD_REMOVAL_RULES_TEXT, detect_military_recruitment, render_military_recruitment_block_notice
from services.media_store import merge_photo_files, save_photo_files_from_attachments
from services.order_service import OrderService
from utils.db import ensure_json_file, read_json, write_json_atomic


VK_MESSAGE_LIMIT = 4096

USER_STATES: dict[str, dict[str, Any]] = {}
PERSISTENT_CALLBACK_COMMANDS = {
    "accept_agreement",
    "mention_all_yes",
    "mention_all_no",
    "view_user_ad",
    "start_extend",
    "contact_support",
    "add_check",
    "submit_order",
    "reupload_check",
    "edit_ad_text",
    "edit_ad_photo",
    "add_ad_photo",
    "edit_main_text",
    "edit_main_photo",
    "add_main_photo",
}

MIN_RATE_PRICE = min(int(data.get("price", 0) or 0) for data in RATES.values())
BASE_DIR = Path(__file__).resolve().parent.parent
USERS_DB_PATH = BASE_DIR / "users_db.json"
AGREEMENT_VERSION = 3
MSK = timezone(timedelta(hours=3))
ensure_json_file(USERS_DB_PATH, {})


def now_msk() -> datetime:
    return datetime.now(MSK)


def now_msk_str() -> str:
    return now_msk().strftime("%Y-%m-%d %H:%M:%S")


WELCOME_TEXT = (
    "👋 Добро пожаловать в Klinoff | PIAR!\n"
    "Поможем запустить рекламу вашего канала, аккаунта или проекта быстро и без лишней переписки.\n\n"
    "Нажмите кнопку ниже, чтобы выбрать действие:"
)

HELP_TEXT = (
    "📋 КАК ЭТО РАБОТАЕТ\n\n"
    "• `.начать` — открыть главное меню\n"
    "• `.правила` — показать соглашение\n"
    "• `.купить` — оформить новую рекламу\n"
    "• `.продлить` — продлить активную рекламу\n"
    "• `.сообщение` — открыть ваши объявления\n"
    "• `.помощь` — показать эту подсказку\n\n"
    "Если удобнее, можно пользоваться кнопками ниже."
)

INFO_TEXT = (
    "ℹ️ О СЕРВИСЕ\n\n"
    "Klinoff | PIAR помогает продвигать каналы, аккаунты и проекты через рекламные рассылки.\n\n"
    "Почему выбирают нас:\n"
    "• 150+ чатов для размещения\n"
    f"• тарифы от {MIN_RATE_PRICE} ₽\n"
    "• быстрое рассмотрение заявок\n"
    "• удобное управление рекламой прямо в ЛС\n\n"
    "Связь: @klinoffshop"
)

PAYMENT_TEXT = (
    "💳 ОПЛАТА ЗАКАЗА\n\n"
    "Переведите сумму переводом на карту:\n"
    f"• {PAYMENT_DETAILS}\n\n"
    "После оплаты нажмите кнопку «📎 Загрузить чек» и отправьте скриншот или файл."
)

AGREEMENT_TEXT = (
    "📜 СОГЛАШЕНИЕ\n\n"
    "Перед использованием сервиса подтвердите согласие с правилами:\n\n"
    "1. Нажимая кнопку принятия, пользователь присоединяется к условиям соглашения в целом. "
    "Условия определены сервисом как стандартная форма; стороны свободны заключать договор и определять его условия, "
    "если они не противоречат закону. Согласие в электронном сообщении/кнопке считается акцептом условий (ст. 421, 428, 434, 438 ГК РФ).\n"
    "2. Услуга является возмездным оказанием услуг: Klinoff Piar и владелец бота по заданию пользователя технически размещают/рассылают переданный пользователем рекламный материал, "
    "а пользователь оплачивает услугу в выбранном тарифе (ст. 779, 781 ГК РФ).\n"
    "3. Пользователь является единственным инициатором размещения, самостоятельно определяет объект рекламирования, текст, ссылки, вложения, изображения, обещания и рекламируемый ресурс. "
    "В этой части пользователь выступает рекламодателем и несёт ответственность за содержание, достоверность и законность материала. Klinoff Piar и владелец бота не являются продавцом, производителем, представителем, партнёром, гарантом, платёжным агентом, организатором деятельности рекламируемого ресурса и не подтверждают законность предложения пользователя "
    "(ст. 3, 5, 38 Федерального закона от 13.03.2006 N 38-ФЗ «О рекламе»; ст. 1064 ГК РФ).\n"
    "4. Пользователь гарантирует, что его реклама, вложения, ссылки и рекламируемый ресурс не содержат запрещённой информации, незаконных товаров/услуг, недостоверных сведений, нарушений прав третьих лиц, "
    "экстремистских, мошеннических, наркотических, порнографических, вредоносных материалов, персональных данных без правового основания и иной информации, за распространение которой предусмотрена гражданская, административная или уголовная ответственность. "
    "Если из-за материала пользователя к Klinoff Piar, владельцу бота, администраторам, сообществу или чатам предъявлены претензии, жалобы, требования, штрафы, блокировки, судебные и внесудебные расходы либо убытки, пользователь обязан самостоятельно урегулировать такие требования, предоставить подтверждающие документы и компенсировать все расходы/убытки в пределах закона "
    "(ст. 5, 7, 38 Закона о рекламе; ст. 10 Федерального закона от 27.07.2006 N 149-ФЗ; ст. 15, 393, 400, 401, 1064 ГК РФ; ст. 14.3 КоАП РФ).\n"
    "5. Модерация заявок является технической и организационной мерой снижения рисков, а не юридической экспертизой и не подтверждением законности рекламы. Klinoff Piar и владелец бота вправе без возврата оплаты за уже оказанную часть услуги отказать в размещении, остановить или удалить рекламу при подозрении на нарушение закона, правил VK, прав третьих лиц, жалобах, блокировках, требованиях площадок, госорганов или иных лиц "
    "(ст. 421 ГК РФ; ст. 5, 7, 18, 38 Закона о рекламе; ст. 10 Закона N 149-ФЗ).\n"
    "6. Оплаченная услуга считается размещением/рассылкой рекламы в базе чатов сервиса. Klinoff Piar и владелец бота не гарантируют конкретный коммерческий результат: приток людей, продажи, переходы, охваты или подписки "
    "(ст. 779, 781 ГК РФ).\n"
    "7. После начала размещения/рассылки возврат средств исключается в части фактически оказанных услуг и понесённых расходов. "
    "При отказе пользователя от услуги применяются правила об оплате фактически понесённых расходов исполнителя (ст. 782 ГК РФ).\n"
    "8. Klinoff Piar и владелец бота не отвечают за блокировки сообществ, бесед, аккаунтов, удаление сообщений, ограничения VK и иные действия платформы или третьих лиц, если они находятся вне контроля сервиса "
    "(ст. 401 ГК РФ; ст. 10 Закона N 149-ФЗ).\n"
    "9. Требования о возмещении убытков возможны только при наличии доказанного нарушения, причинной связи, размера убытков и оснований ответственности по закону (ст. 15, 393, 401, 1064 ГК РФ).\n\n"
    f"{AD_REMOVAL_RULES_TEXT}\n\n"
    "Принимая данное соглашение, пользователь подтверждает, что принимает на себя ответственность за свои материалы, а Klinoff Piar и владелец бота освобождаются от ответственности в максимально допустимых законодательством РФ пределах."
)


def _normalize_command(raw: str) -> str:
    value = (raw or "").strip().lower()
    if value.startswith("."):
        value = value[1:]
    return value


def _parse_payload(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("payload")
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str) and payload.strip():
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return {}


def _load_users_db() -> dict[str, Any]:
    return read_json(USERS_DB_PATH, {})


def _save_users_db(data: dict[str, Any]) -> None:
    write_json_atomic(USERS_DB_PATH, data)


def _has_accepted_agreement(user_id: int) -> bool:
    users = _load_users_db()
    user = users.get(str(user_id), {})
    return int(user.get("agreement_version", 0) or 0) >= AGREEMENT_VERSION


def _accept_agreement(user_id: int) -> None:
    users = _load_users_db()
    user_key = str(user_id)
    user = users.setdefault(user_key, {})
    user["agreement_version"] = AGREEMENT_VERSION
    user["agreement_accepted_at"] = now_msk_str()
    _save_users_db(users)


def _answer_callback(vk, event_object: dict[str, Any] | None, *, text: str | None = None, link: str | None = None, logger=None) -> None:
    if not event_object:
        return
    event_data = {"type": "open_link", "link": link} if link else {"type": "show_snackbar", "text": text or "Открываю..."}
    try:
        vk.messages.sendMessageEventAnswer(
            event_id=event_object["event_id"],
            user_id=event_object["user_id"],
            peer_id=event_object["peer_id"],
            event_data=json.dumps(event_data, ensure_ascii=False),
        )
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка ответа на callback: {exc}")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
    except ValueError:
        return None


def _extract_attachment(message: dict[str, Any]) -> dict[str, Any] | None:
    attachments = message.get("attachments") or []
    if not attachments:
        return None
    raw = attachments[0]
    kind = raw.get("type")
    if kind not in {"photo", "video", "doc"}:
        return None
    item = raw[kind]
    ref = f"{kind}{item['owner_id']}_{item['id']}"
    if item.get("access_key"):
        ref = f"{ref}_{item['access_key']}"
    result = {"type": kind, "vk_attachment": ref, "payload": raw}
    if kind == "doc":
        result["title"] = item.get("title")
        result["url"] = item.get("url")
        result["ext"] = item.get("ext")
    return result


def _split_message(text: str, limit: int = VK_MESSAGE_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_pos = text.rfind("\n", 0, limit)
        if split_pos == -1:
            split_pos = limit
        chunks.append(text[:split_pos])
        text = text[split_pos:].lstrip("\n")
    return chunks


def _send(vk, user_id: int, text: str, service: OrderService, *, keyboard: str | None = None, attachment: str | None = None, logger=None):
    chunks = _split_message(text or " ")
    result = None
    for i, chunk in enumerate(chunks):
        params = {"user_id": user_id, "message": chunk, "random_id": service.random_id()}
        if keyboard and i == len(chunks) - 1:
            params["keyboard"] = keyboard
        if attachment and i == len(chunks) - 1:
            params["attachment"] = attachment
        try:
            result = vk.messages.send(**params)
        except Exception as exc:
            if logger:
                logger.error(f"Ошибка отправки ЛС пользователю {user_id}: {exc}")
            return None
    return result


def _delete_pm_message(vk, peer_id: int, event_object: dict[str, Any] | None, logger=None) -> None:
    if not event_object:
        return
    try:
        cmid = event_object.get("conversation_message_id")
        if cmid:
            vk.messages.delete(peer_id=peer_id, cmids=cmid, delete_for_all=1)
            return
    except Exception:
        pass
    try:
        message_id = event_object.get("message_id")
        if message_id:
            vk.messages.delete(message_ids=message_id, delete_for_all=1)
    except Exception as exc:
        if logger:
            logger.warning(f"Не удалось удалить ЛС-сообщение {event_object}: {exc}")


def _check_subscription(vk, user_id: int, logger=None) -> bool:
    try:
        return bool(vk.groups.isMember(group_id=group_id, user_id=user_id))
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка проверки подписки: {exc}")
        return False


def _rate_days_from_text(text: str) -> int | None:
    for days, data in RATES.items():
        if text == data.get("name") or text == f"{data['name']} — {data['price']} ₽":
            return int(days)
    return None


def _contains_mass_mention(text: str | None) -> bool:
    value = (text or "").lower()
    return "@all" in value or "@все" in value


def _normalize_button_link(link: str | None) -> str:
    value = (link or "").strip()
    if value and "://" not in value:
        value = f"https://{value}"
    return value


def _has_custom_button(flow: dict[str, Any]) -> bool:
    return bool(flow.get("button_title") and flow.get("button_link"))


def _can_buy_mention_all(days: int | None) -> bool:
    return int(days or 0) >= 30


def _prepend_mass_mention(text: str | None) -> str:
    value = (text or "").strip()
    if _contains_mass_mention(value):
        return value
    return "@all" if not value else f"@all {value}"



def _get_user_name(vk, user_id: int) -> str:
    try:
        user = vk.users.get(user_ids=user_id)[0]
        return f"{user['first_name']} {user['last_name']}"
    except Exception:
        return f"Пользователь {user_id}"


def _get_user_mention(vk, user_id: int) -> str:
    return f"[id{user_id}|{_get_user_name(vk, user_id)}]"


def _next_order_id(orders_data: dict[str, Any]) -> str:
    order_id = f"order_{int(time.time())}"
    while order_id in orders_data:
        order_id = f"order_{int(time.time() * 1000)}"
    return order_id


def _next_order_no(service: OrderService, orders_data: dict[str, Any]) -> int:
    return service.next_order_number(orders_data)


def _next_order_code(service: OrderService, orders_data: dict[str, Any]) -> str:
    return service.next_order_code(orders_data)


def _append_photo_attachment(existing: str | None, new_attachment: str | None) -> str | None:
    if not new_attachment:
        return existing
    parts = [part.strip() for part in str(existing or "").split(",") if part.strip()]
    if new_attachment not in parts:
        parts.append(new_attachment)
    return ",".join(parts) if parts else None


def _cache_ad_photo_files(message: dict[str, Any], prefix: str, logger=None) -> list[str | None]:
    return save_photo_files_from_attachments(
        BASE_DIR,
        message.get("attachments") or [],
        prefix=prefix,
        allowed_kinds={"photo", "video"},
        logger=logger,
    )


def _find_pending_edit_order(
    orders_data: dict[str, Any],
    *,
    user_id: int,
    item_key: str,
    order_type: str,
) -> tuple[str | None, dict[str, Any] | None]:
    for order_id, order in orders_data.items():
        if not isinstance(order, dict):
            continue
        if order.get("status") != "pending":
            continue
        if order.get("user_id") != user_id:
            continue
        if order.get("type") != order_type:
            continue
        if order.get("item_key") != item_key:
            continue
        return order_id, order
    return None, None


def _upsert_pending_edit_order(
    service: OrderService,
    orders_data: dict[str, Any],
    *,
    user_id: int,
    item_key: str,
    order_type: str,
    text: str,
    photo: str | None,
    photo_files: list[str | None] | None,
    price: int,
    days: int,
    rate_name: str | None,
    base_item: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    now = now_msk_str()
    order_id, order = _find_pending_edit_order(
        orders_data,
        user_id=user_id,
        item_key=item_key,
        order_type=order_type,
    )
    created = False
    if order is None or order_id is None:
        order_id = _next_order_id(orders_data)
        order = {
            "order_no": _next_order_no(service, orders_data),
            "order_code": _next_order_code(service, orders_data),
            "type": order_type,
            "status": "pending",
            "user_id": user_id,
            "created_at": now,
            "events": [],
        }
        orders_data[order_id] = order
        created = True

    order["item_key"] = item_key
    if base_item:
        order["source_order_id"] = base_item.get("order_id")
        order["source_order_code"] = base_item.get("order_code")
    order["text"] = text
    order["photo"] = photo
    order["photo_files"] = photo_files or []
    order["price"] = price
    order["days"] = days
    order["rate_name"] = rate_name
    order["updated_at"] = now
    order.setdefault("events", []).append({"at": now, "event": "created" if created else "updated"})
    return order_id, created


def _get_effective_active_ads(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for key, ad_data in ads_data.items():
        if key in {"main_text_sale", "active_ad", "users"}:
            continue
        if not isinstance(ad_data, dict):
            continue
        if ad_data.get("user_id") != user_id:
            continue
        expires_at = _parse_dt(ad_data.get("expires_at"))
        if expires_at is not None and expires_at < now_msk():
            continue
        if ad_data.get("status") not in {"approved", "active", None}:
            continue
        ad_key = ad_data.get("ad_key") or key
        order_id = ad_data.get("order_id") or ""
        # Дедупликация: по ad_key И по order_id (одна реклама может быть под двумя ключами)
        if ad_key in seen_keys or (order_id and order_id in seen_keys):
            continue
        seen_keys.add(ad_key)
        if order_id:
            seen_keys.add(order_id)
        normalized = dict(ad_data)
        normalized["ad_key"] = ad_key
        items.append(normalized)
    items.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    return items


def _count_user_extra_ads(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> int:
    return len(_get_effective_active_ads(service, ads_data, orders_data, user_id))


def _active_items(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for idx, active in enumerate(_get_effective_active_ads(service, ads_data, orders_data, user_id), start=1):
        items.append(
            {
                "key": active.get("ad_key") or f"extra_{idx}",
                "order_id": active.get("order_id"),
                "order_code": active.get("order_code"),
                "title": f"📢 Дополнительная реклама #{idx}",
                "button_title": f"📢 Объявление #{idx}",
                "extend_title": f"⏳ Продлить объявление #{idx}",
                "type": "extra",
                "text": active.get("text", "") or "Текст не указан",
                "photo": active.get("photo"),
                "photo_files": active.get("photo_files"),
                "custom_button_title": active.get("button_title"),
                "custom_button_link": active.get("button_link"),
                "price": active.get("price", 0),
                "days": active.get("days", 0),
                "rate_name": active.get("rate_name"),
                "expires_at": active.get("expires_at", "не указано"),
            }
        )
    main_text_sale = ads_data.get("main_text_sale")
    if isinstance(main_text_sale, dict) and main_text_sale.get("user_id") == user_id:
        expires_at = _parse_dt(main_text_sale.get("expires_at"))
        if expires_at is None or expires_at >= now_msk():
            items.append(
                {
                    "key": "main_text_sale",
                    "order_id": main_text_sale.get("order_id"),
                    "order_code": main_text_sale.get("order_code"),
                    "title": "💎 Основная реклама",
                    "button_title": "💎 Основной блок",
                    "extend_title": "⏳ Продлить основной блок",
                    "type": "main",
                    "text": main_text_sale.get("text", "") or "Текст не указан",
                    "photo": main_text_sale.get("photo"),
                    "photo_files": main_text_sale.get("photo_files"),
                    "custom_button_title": main_text_sale.get("button_title"),
                    "custom_button_link": main_text_sale.get("button_link"),
                    "price": main_text_sale.get("price", 0),
                    "days": main_text_sale.get("days", 0),
                    "rate_name": main_text_sale.get("rate_name"),
                    "expires_at": main_text_sale.get("expires_at", "не указано"),
                }
            )
    return items


def _main_menu(service: OrderService, ads_data: dict[str, Any], orders_data: dict[str, Any], user_id: int) -> str:
    items = _active_items(service, ads_data, orders_data, user_id)
    has_active_ad = bool(items)
    return generate_main_menu(has_active_ad=has_active_ad, has_messages=has_active_ad)


def _kb_cancel() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"command": "cancel"})
    return kb.get_keyboard()


def _kb_yes_skip() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("✅ Да", color=VkKeyboardColor.POSITIVE, payload={"command": "add_media_yes"})
    kb.add_callback_button("➡️ Пропустить", color=VkKeyboardColor.PRIMARY, payload={"command": "skip_media"})
    return kb.get_keyboard()


def _kb_agreement(accepted: bool) -> str:
    kb = VkKeyboard(inline=True)
    if not accepted:
        kb.add_callback_button("✅ Принимаю соглашение", color=VkKeyboardColor.POSITIVE, payload={"command": "accept_agreement"})
        kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()




def _kb_mention_all() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button(f"➕ Добавить @all/@все (+{MENTION_ALL_PRICE} ₽)", color=VkKeyboardColor.POSITIVE, payload={"command": "mention_all_yes"})
    kb.add_line()
    kb.add_callback_button("🚫 Без @all/@все", color=VkKeyboardColor.SECONDARY, payload={"command": "mention_all_no"})
    kb.add_line()
    kb.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"command": "cancel"})
    return kb.get_keyboard()


def _payment_text(flow: dict[str, Any]) -> str:
    button_extra = int(flow.get("custom_button_price", 0) or 0)
    extra = int(flow.get("mention_all_price", 0) or 0)
    mention_label = "включено" if flow.get("mention_all") else "не включено"
    total = int(flow.get("price", 0) or 0) + extra + button_extra
    mention_line = f"@all/@все: {mention_label}" + (f" (+{extra} ₽)" if extra else "")
    return PAYMENT_TEXT + f"\n\n{mention_line}\nИтого к оплате: {total} ₽"


def _kb_custom_button() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button(f"✅ Добавить кнопку (+{CUSTOM_BUTTON_PRICE} ₽)", color=VkKeyboardColor.POSITIVE, payload={"command": "custom_button_yes"})
    kb.add_line()
    kb.add_callback_button("➡️ Без кнопки", color=VkKeyboardColor.SECONDARY, payload={"command": "custom_button_no"})
    kb.add_line()
    kb.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"command": "cancel"})
    return kb.get_keyboard()


def _ask_custom_button(vk, user_id: int, flow: dict[str, Any], service: OrderService, logger=None) -> None:
    flow["step"] = "wait_custom_button_choice"
    _send(
        vk,
        user_id,
        f"Хотите ли вы добавить уникальную кнопку под сообщение?\nДанная услуга стоит {CUSTOM_BUTTON_PRICE} рублей.",
        service,
        keyboard=_kb_custom_button(),
        logger=logger,
    )


def _payment_text(flow: dict[str, Any]) -> str:
    extra = int(flow.get("mention_all_price", 0) or 0)
    button_extra = int(flow.get("custom_button_price", 0) or 0)
    mention_label = "включено" if flow.get("mention_all") else "не включено"
    button_label = "включена" if _has_custom_button(flow) else "не включена"
    total = int(flow.get("price", 0) or 0) + extra + button_extra
    mention_line = f"@all/@все: {mention_label}" + (f" (+{extra} ₽)" if extra else "")
    button_line = f"Уникальная кнопка: {button_label}" + (f" (+{button_extra} ₽)" if button_extra else "")
    return PAYMENT_TEXT + f"\n\n{mention_line}\n{button_line}\nИтого к оплате: {total} ₽"


def _kb_buy_menu() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("📢 Дополнительная реклама", color=VkKeyboardColor.PRIMARY, payload={"command": "buy_extra_ad_info"})
    kb.add_line()
    kb.add_callback_button("💎 Основная реклама", color=VkKeyboardColor.POSITIVE, payload={"command": "buy_main_ad_info"})
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()


def _kb_ready_back(ready_command: str) -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("✅ Продолжить оформление", color=VkKeyboardColor.POSITIVE, payload={"command": ready_command})
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "buy_menu"})
    return kb.get_keyboard()


def _kb_payment() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("📎 Загрузить чек", color=VkKeyboardColor.POSITIVE, payload={"command": "add_check"})
    kb.add_line()
    kb.add_callback_button("❌ Отмена", color=VkKeyboardColor.NEGATIVE, payload={"command": "cancel"})
    return kb.get_keyboard()


def _kb_check_confirm() -> str:
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("✅ Да, отправить", color=VkKeyboardColor.POSITIVE, payload={"command": "submit_order"})
    kb.add_line()
    kb.add_callback_button("❌ Нет, изменить", color=VkKeyboardColor.NEGATIVE, payload={"command": "reupload_check"})
    return kb.get_keyboard()


def _kb_info(can_buy: bool) -> str:
    kb = VkKeyboard(inline=True)
    if can_buy:
        kb.add_callback_button("🚀 Купить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "buy_menu"})
        kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()


def _kb_active_items(items: list[dict[str, Any]]) -> str:
    kb = VkKeyboard(inline=True)
    for index, item in enumerate(items, start=1):
        kb.add_callback_button(item["button_title"], color=VkKeyboardColor.PRIMARY, payload={"command": "view_user_ad", "item_key": item["key"]})
        if index < len(items):
            kb.add_line()
    return kb.get_keyboard()


def _kb_extend_items(items: list[dict[str, Any]]) -> str:
    kb = VkKeyboard(inline=True)
    for index, item in enumerate(items, start=1):
        kb.add_callback_button(item["extend_title"], color=VkKeyboardColor.POSITIVE, payload={"command": "start_extend", "item_key": item["key"]})
        if index < len(items):
            kb.add_line()
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".начать"})
    return kb.get_keyboard()


def _kb_active_item_actions(item: dict[str, Any]) -> str:
    kb = VkKeyboard(inline=True)
    if item.get("type") == "extra":
        kb.add_callback_button("✏️ Редактировать текст", color=VkKeyboardColor.PRIMARY, payload={"command": "edit_ad_text", "item_key": item["key"]})
        kb.add_callback_button("🖼️ Редактировать фото", color=VkKeyboardColor.SECONDARY, payload={"command": "edit_ad_photo", "item_key": item["key"]})
        kb.add_line()
        kb.add_callback_button("📎 Добавить фото", color=VkKeyboardColor.SECONDARY, payload={"command": "add_ad_photo", "item_key": item["key"]})
        kb.add_line()
    if item.get("type") == "main":
        kb.add_callback_button("✏️ Редактировать текст", color=VkKeyboardColor.PRIMARY, payload={"command": "edit_main_text", "item_key": item["key"]})
        kb.add_callback_button("🖼️ Редактировать фото", color=VkKeyboardColor.SECONDARY, payload={"command": "edit_main_photo", "item_key": item["key"]})
        kb.add_line()
        kb.add_callback_button("📎 Добавить фото", color=VkKeyboardColor.SECONDARY, payload={"command": "add_main_photo", "item_key": item["key"]})
        kb.add_line()
    kb.add_callback_button("⏳ Продлить рекламу", color=VkKeyboardColor.POSITIVE, payload={"command": "start_extend", "item_key": item["key"]})
    kb.add_line()
    kb.add_callback_button("🆘 Связаться с поддержкой", color=VkKeyboardColor.NEGATIVE, payload={"command": "contact_support", "item_key": item["key"]})
    kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": ".сообщение"})
    return kb.get_keyboard()


def _render_active_item(item: dict[str, Any]) -> str:
    return (
        f"{item['title']}\n\n"
        f"📝 Текст:\n{item.get('text', '') or 'Текст не указан'}\n\n"
        f"📅 Активно до: {item.get('expires_at', 'не указано')}\n"
        f"💳 Тариф: {item.get('rate_name') or '-'}"
    )


def _extra_ad_description() -> str:
    return (
        "📢 ДОПОЛНИТЕЛЬНАЯ РЕКЛАМА\n\n"
        "Ваш текст попадёт в общую рассылку по чатам.\n"
        "Сообщения распределяются вместе с другими заказами в случайном порядке.\n"
        "При этом реклама всё равно будет отправлена по всей базе чатов.\n\n"
        "Если формат вам подходит, нажмите «Продолжить оформление»."
    )


def _main_ad_description(can_buy: bool, reason: str | None) -> str:
    if not can_buy:
        return (
            "💎 ОСНОВНАЯ РЕКЛАМА\n\n"
            "Основной рекламный текст отправляется отдельным заметным блоком в конце рассылки.\n\n"
            f"⚠️ Сейчас место занято.\n{reason}"
        )
    return (
        "💎 ОСНОВНАЯ РЕКЛАМА\n\n"
        "Этот формат размещается отдельным блоком в конце рассылки.\n"
        "Он заметнее обычной дополнительной рекламы и лучше выделяется в ленте сообщений.\n\n"
        "Если формат вам подходит, нажмите «Продолжить оформление»."
    )


def _notify_admin(vk, message_config: dict[str, Any], service: OrderService, *, extension_only: bool = False, logger=None):
    admin_chat = message_config.get("admin_chat")
    if not admin_chat:
        return
    text = "📥 НОВАЯ ЗАЯВКА НА ПРОДЛЕНИЕ\nИспользуйте .заказы для просмотра" if extension_only else "📥 НОВАЯ ЗАЯВКА\nИспользуйте .заказы для просмотра"
    try:
        vk.messages.send(peer_id=admin_chat, message=text, random_id=service.random_id())
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка отправки уведомления в админ-чат: {exc}")


def _notify_support_request(vk, message_config: dict[str, Any], service: OrderService, user_id: int, item: dict[str, Any], logger=None):
    admin_chat = message_config.get("admin_chat")
    if not admin_chat:
        return
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("💬 Перейти в чат", color=VkKeyboardColor.PRIMARY, payload={"command": "support_open_chat", "user_id": user_id})
    kb.add_line()
    kb.add_callback_button("✅ Закрыто", color=VkKeyboardColor.POSITIVE, payload={"command": "support_close", "user_id": user_id})
    try:
        vk.messages.send(
            peer_id=admin_chat,
            message=(
                "🆘 Пользователь связался со службой поддержки\n\n"
                f"Покупатель: [id{user_id}|Пользователь]\n"
                f"Реклама: {item.get('title', 'не указано')}\n"
                f"Текст: {(item.get('text', '') or 'Текст не указан')[:300]}\n"
                f"Причина: {item.get('support_reason', 'не указана')}"
            ),
            keyboard=kb.get_keyboard(),
            random_id=service.random_id(),
        )
    except Exception as exc:
        if logger:
            logger.error(f"Ошибка отправки заявки в поддержку в админ-чат: {exc}")


def _reset_state(state: dict[str, Any]) -> None:
    flow = state.get("flow")
    support_mode = state.get("support_mode")
    admin_order_edit = state.get("admin_order_edit")
    state.clear()
    if support_mode:
        state["support_mode"] = support_mode
    if admin_order_edit:
        state["admin_order_edit"] = admin_order_edit
    if flow and flow.get("persistent"):
        state["flow"] = flow


def _reject_flow_for_safety(
    vk,
    user_id: int,
    service: OrderService,
    ads_data: dict[str, Any],
    orders_data: dict[str, Any],
    state: dict[str, Any],
    safety: dict[str, Any],
    *,
    order_code: str | None = None,
    logger=None,
) -> None:
    _send(
        vk,
        user_id,
        render_military_recruitment_block_notice(order_code=order_code, reason=safety.get("reason")),
        service,
        keyboard=_main_menu(service, ads_data, orders_data, user_id),
        logger=logger,
    )
    _reset_state(state)


def _find_item(items: list[dict[str, Any]], item_key: str | None) -> dict[str, Any] | None:
    for item in items:
        if item.get("key") == item_key:
            return item
    return None


def handle_personal_message(
    vk,
    event,
    user_id: int,
    ads_data: dict[str, Any],
    orders_data: dict[str, Any],
    message_config: dict[str, Any],
    service: OrderService,
    save_ads,
    save_orders,
    logger=None,
):
    message = event.obj.message
    payload = _parse_payload(message)
    text = (message.get("text") or "").strip()
    raw_command = payload.get("command") or text
    command = _normalize_command(raw_command)
    event_object = message.get("_event_object")
    attachment = _extract_attachment(message)

    if event_object and command not in PERSISTENT_CALLBACK_COMMANDS:
        _delete_pm_message(vk, user_id, event_object, logger=logger)

    state = USER_STATES.setdefault(str(user_id), {"menu_sent": False})
    flow = state.get("flow")
    agreement_accepted = _has_accepted_agreement(user_id)

    if command == "правила":
        _send(vk, user_id, AGREEMENT_TEXT, service, keyboard=_kb_agreement(agreement_accepted), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "accept_agreement":
        _accept_agreement(user_id)
        _reset_state(state)
        if event_object:
            _delete_pm_message(vk, user_id, event_object, logger=logger)
        acceptance_text = (
            f"{_get_user_mention(vk, user_id)}, принял соглашение и подтверждает ответственность за свои материалы; "
            "Klinoff Piar и владелец бота освобождаются от ответственности в пределах законодательства РФ."
        )
        _send(vk, user_id, acceptance_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, text="Соглашение принято", logger=logger)
        return

    if not agreement_accepted:
        _reset_state(state)
        _send(
            vk,
            user_id,
            f"{AGREEMENT_TEXT}\n\nЧтобы продолжить, нажмите «✅ Принимаю соглашение».\nПовторно открыть текст можно командой `.правила`.",
            service,
            keyboard=_kb_agreement(False),
            logger=logger,
        )
        if event_object:
            _answer_callback(vk, event_object, text="Сначала примите соглашение", logger=logger)
        return

    if state.get("support_mode"):
        return

    if command == "cancel":
        _reset_state(state)
        _send(vk, user_id, WELCOME_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, text="Отменено", logger=logger)
        return

    if command in {"начать", "start"}:
        _reset_state(state)
        _send(vk, user_id, WELCOME_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "помощь":
        _send(vk, user_id, HELP_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "инфо_о_нас":
        can_buy, reason = service.can_buy_main_text(ads_data)
        msg = INFO_TEXT if can_buy else f"{INFO_TEXT}\n\n⚠️ Место основного текста занято.\n{reason}"
        _send(vk, user_id, msg, service, keyboard=_kb_info(can_buy), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "сообщение":
        items = _active_items(service, ads_data, orders_data, user_id)
        if not items:
            _send(vk, user_id, "📭 У вас пока нет активной рекламы.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        _send(vk, user_id, "📂 ВАШИ АКТИВНЫЕ ОБЪЯВЛЕНИЯ\n\nВыберите объявление, чтобы посмотреть детали и управлять им:", service, keyboard=_kb_active_items(items), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "продлить":
        items = _active_items(service, ads_data, orders_data, user_id)
        if not items:
            _send(vk, user_id, "📭 У вас пока нет активной рекламы для продления.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        _send(vk, user_id, "⏳ Выберите рекламу, которую хотите продлить:", service, keyboard=_kb_extend_items(items), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command in {"купить", "buy_menu"}:
        _reset_state(state)
        if _count_user_extra_ads(service, ads_data, orders_data, user_id) >= 3:
            _send(
                vk,
                user_id,
                "⚠️ У вас уже максимум дополнительных реклам.\n\nМожно купить не больше 3 доп. текстов одновременно.",
                service,
                keyboard=_main_menu(service, ads_data, orders_data, user_id),
                logger=logger,
            )
            return
        _send(vk, user_id, "🚀 Выберите формат рекламы:", service, keyboard=_kb_buy_menu(), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "buy_extra_ad_info":
        _send(vk, user_id, _extra_ad_description(), service, keyboard=_kb_ready_back("ready_buy_extra"), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "buy_main_ad_info":
        can_buy, reason = service.can_buy_main_text(ads_data)
        _send(vk, user_id, _main_ad_description(can_buy, reason), service, keyboard=_kb_ready_back("ready_buy_main" if can_buy else "buy_menu"), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "ready_buy_extra":
        _send(vk, user_id, "📅 Выберите срок рассылки:", service, keyboard=generate_rates_menu(), logger=logger)
        return

    if command == "ready_buy_main":
        _send(vk, user_id, "📅 Выберите срок основной рекламы:", service, keyboard=generate_main_text_rates_menu(), logger=logger)
        return

    items = _active_items(service, ads_data, orders_data, user_id)

    if command == "view_user_ad":
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        _send(vk, user_id, _render_active_item(item), service, keyboard=_kb_active_item_actions(item), attachment=item.get("photo"), logger=logger)
        if event_object:
            _answer_callback(vk, event_object, logger=logger)
        return

    if command == "start_extend":
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        state["flow"] = {"step": "wait_extend_rate", "base_item": item, "item_key": item["key"], "mode": "renew_main_text" if item.get("type") == "main" else "extend"}
        if item.get("type") == "main":
            _send(vk, user_id, "📅 Выберите срок продления основной рекламы:", service, keyboard=generate_main_text_rates_menu(), logger=logger)
        else:
            _send(vk, user_id, "📅 Выберите срок продления:", service, keyboard=generate_rates_menu(), logger=logger)
        return

    if command == "contact_support":
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        state["flow"] = {"step": "wait_support_reason", "base_item": item, "item_key": item["key"]}
        _send(vk, user_id, "✍️ Перед вызовом администратора напишите причину обращения следующим сообщением.", service, keyboard=_kb_cancel(), logger=logger)
        return

    if command in {"edit_ad_text", "edit_ad_photo", "add_ad_photo", "edit_main_text", "edit_main_photo", "add_main_photo"}:
        item = _find_item(items, payload.get("item_key"))
        if not item:
            _send(vk, user_id, "❌ Реклама не найдена.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            return
        if command == "edit_ad_text":
            state["flow"] = {"step": "wait_update_text", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "photo_files": item.get("photo_files"), "days": item.get("days", 0), "rate_name": item.get("rate_name")}
            _send(vk, user_id, "✏️ Пришлите новый текст для рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "edit_main_text":
            state["flow"] = {"step": "wait_update_main_text", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "photo_files": item.get("photo_files"), "days": item.get("days", 0), "price": item.get("price", 0), "rate_name": item.get("rate_name")}
            _send(vk, user_id, "✏️ Пришлите новый текст для основной рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "edit_ad_photo":
            state["flow"] = {"step": "wait_update_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "photo_files": item.get("photo_files"), "days": item.get("days", 0), "rate_name": item.get("rate_name"), "append_photo": False}
            _send(vk, user_id, "🖼️ Пришлите новое фото для рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "edit_main_photo":
            state["flow"] = {"step": "wait_update_main_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "photo_files": item.get("photo_files"), "days": item.get("days", 0), "price": item.get("price", 0), "rate_name": item.get("rate_name"), "append_photo": False}
            _send(vk, user_id, "🖼️ Пришлите новое фото для основной рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "add_ad_photo":
            state["flow"] = {"step": "wait_update_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "photo_files": item.get("photo_files"), "days": item.get("days", 0), "rate_name": item.get("rate_name"), "append_photo": True}
            _send(vk, user_id, "📎 Пришлите дополнительное фото для рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "add_main_photo":
            state["flow"] = {"step": "wait_update_main_photo", "base_item": item, "item_key": item["key"], "text": item.get("text", ""), "photo": item.get("photo"), "photo_files": item.get("photo_files"), "days": item.get("days", 0), "price": item.get("price", 0), "rate_name": item.get("rate_name"), "append_photo": True}
            _send(vk, user_id, "📎 Пришлите дополнительное фото для основной рекламы.", service, keyboard=_kb_cancel(), logger=logger)
            return

    if flow and flow.get("step") == "wait_support_reason" and text and not text.startswith("."):
        item = dict(flow.get("base_item") or {})
        item["support_reason"] = text
        _notify_support_request(vk, message_config, service, user_id, item, logger=logger)
        _send(vk, user_id, "✅ Заявка в поддержку отправлена. Ожидайте администратора.", service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_extend_rate":
        selected_days = payload.get("days")
        if selected_days is None:
            selected_days = _rate_days_from_text(text)
        if selected_days is not None:
            selected_days = int(selected_days)
            base_item = flow.get("base_item") or {}
            rates_map = MAIN_TEXT_RATES if base_item.get("type") == "main" else RATES
            rate = rates_map[selected_days]
            flow.update(
                {
                    "step": "wait_mention_all",
                    "days": selected_days,
                    "price": rate["price"],
                    "rate_name": rate["name"],
                    "text": base_item.get("text", ""),
                    "photo": base_item.get("photo"),
                    "photo_files": base_item.get("photo_files"),
                    "button_title": base_item.get("custom_button_title"),
                    "button_link": base_item.get("custom_button_link"),
                    "custom_button_price": 0,
                }
            )
            if _can_buy_mention_all(selected_days):
                _send(vk, user_id, f"Хотите добавить @all/@все за +{MENTION_ALL_PRICE} ₽?", service, keyboard=_kb_mention_all(), logger=logger)
            else:
                flow["mention_all"] = False
                flow["mention_all_price"] = 0
                _ask_custom_button(vk, user_id, flow, service, logger=logger)

            return

    if command == "rate_select":
        selected_days = payload.get("days")
        if selected_days is not None:
            selected_days = int(selected_days)
            rate = RATES[selected_days]
            state["flow"] = {"step": "wait_text", "days": selected_days, "price": rate["price"], "rate_name": rate["name"], "text": "", "photo": None, "photo_files": [], "photo_uploaded": False, "mode": "new_ad", "mention_all": False, "mention_all_price": 0, "button_title": None, "button_link": None, "custom_button_price": 0}

            _send(vk, user_id, "✍️ Пришлите текст для рассылки", service, keyboard=_kb_cancel(), logger=logger)
            return

    if command == "buy_main_text":
        selected_days = payload.get("days")
        if selected_days is not None:
            selected_days = int(selected_days)
            rate = MAIN_TEXT_RATES[selected_days]
            state["flow"] = {"step": "wait_text", "days": selected_days, "price": rate["price"], "rate_name": rate["name"], "text": "", "photo": None, "photo_files": [], "photo_uploaded": False, "mode": "main_text", "mention_all": False, "mention_all_price": 0, "button_title": None, "button_link": None, "custom_button_price": 0}

            _send(vk, user_id, "✍️ Пришлите текст для основной рекламы", service, keyboard=_kb_cancel(), logger=logger)
            return

    if flow and flow.get("step") == "wait_text" and text and not text.startswith("."):
        safety = detect_military_recruitment(text)
        if safety.get("blocked"):
            _reject_flow_for_safety(vk, user_id, service, ads_data, orders_data, state, safety, logger=logger)
            return
        flow["text"] = text
        flow["step"] = "choose_media"
        _send(vk, user_id, f"📄 Ваш текст:\n\n{text}\n\nДобавить фото?", service, keyboard=_kb_yes_skip(), logger=logger)
        return



    if flow and flow.get("step") == "choose_media":
        if command == "add_media_yes":
            flow["step"] = "wait_media"
            _send(vk, user_id, "📎 Пришлите фото", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "skip_media":
            if _can_buy_mention_all(flow.get("days")):
                flow["step"] = "wait_mention_all"
                _send(vk, user_id, f"Хотите добавить @all/@все за +{MENTION_ALL_PRICE} ₽?", service, keyboard=_kb_mention_all(), logger=logger)
            else:
                flow["mention_all"] = False
                flow["mention_all_price"] = 0
                _ask_custom_button(vk, user_id, flow, service, logger=logger)

            return

    if flow and flow.get("step") == "wait_media" and attachment and attachment["type"] in {"photo", "video"}:
        flow["photo"] = attachment["vk_attachment"]
        flow["photo_files"] = _cache_ad_photo_files(message, f"user_{user_id}_{int(time.time() * 1000)}", logger=logger)
        if _can_buy_mention_all(flow.get("days")):
            flow["step"] = "wait_mention_all"
            _send(vk, user_id, f"Хотите добавить @all/@все за +{MENTION_ALL_PRICE} ₽?", service, keyboard=_kb_mention_all(), logger=logger)
        else:
            flow["mention_all"] = False
            flow["mention_all_price"] = 0
            _ask_custom_button(vk, user_id, flow, service, logger=logger)
        return


    if flow and flow.get("step") == "wait_mention_all":
        if command in {"mention_all_yes", "mention_all_no"}:
            if not _can_buy_mention_all(flow.get("days")):
                flow["mention_all"] = False
                flow["mention_all_price"] = 0
                _ask_custom_button(vk, user_id, flow, service, logger=logger)
                return
            mention_selected = command == "mention_all_yes"
            if not mention_selected and _contains_mass_mention(flow.get("text")):
                _send(
                    vk,
                    user_id,
                    f"⚠️ В тексте найдено @all/@все.\nЧтобы отправить такой текст, нужно оплатить опцию +{MENTION_ALL_PRICE} ₽.",
                    service,
                    keyboard=_kb_mention_all(),
                    logger=logger,
                )
                return
            flow["mention_all"] = mention_selected
            flow["mention_all_price"] = MENTION_ALL_PRICE if mention_selected else 0
            _ask_custom_button(vk, user_id, flow, service, logger=logger)
            return

    if flow and flow.get("step") == "wait_custom_button_choice":
        if command == "custom_button_yes":
            flow["step"] = "wait_custom_button_link"
            _send(vk, user_id, "Отправьте ссылку, на которую нужно перейти при нажатии на кнопку.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "custom_button_no":
            flow["button_title"] = None
            flow["button_link"] = None
            flow["custom_button_price"] = 0
            flow["step"] = "payment"
            _send(vk, user_id, _payment_text(flow), service, keyboard=_kb_payment(), logger=logger)
            return

    if flow and flow.get("step") == "wait_custom_button_link" and text and not text.startswith("."):
        button_link = _normalize_button_link(text)
        if "." not in button_link:
            _send(vk, user_id, "❌ Отправьте корректную ссылку для кнопки.", service, keyboard=_kb_cancel(), logger=logger)
            return
        flow["button_link"] = button_link
        flow["step"] = "wait_custom_button_title"
        _send(vk, user_id, "Теперь отправьте титул кнопки. Например: Написать нам!", service, keyboard=_kb_cancel(), logger=logger)
        return

    if flow and flow.get("step") == "wait_custom_button_title" and text and not text.startswith("."):
        flow["button_title"] = text[:40]
        flow["custom_button_price"] = CUSTOM_BUTTON_PRICE
        flow["step"] = "payment"
        _send(vk, user_id, _payment_text(flow), service, keyboard=_kb_payment(), logger=logger)
        return


    if flow and flow.get("step") == "wait_update_text" and text and not text.startswith(".") and flow.get("base_item") is not None:
        safety = detect_military_recruitment(text)
        if safety.get("blocked"):
            _reject_flow_for_safety(vk, user_id, service, ads_data, orders_data, state, safety, logger=logger)
            return
        if _contains_mass_mention(text):
            _send(
                vk,
                user_id,
                f"⚠️ Нельзя добавлять @all/@все без оплаченной опции (+{MENTION_ALL_PRICE} ₽).\nУдалите упоминание и отправьте текст снова.",
                service,
                keyboard=_kb_cancel(),
                logger=logger,
            )
            return
        flow["text"] = text
        _, created = _upsert_pending_edit_order(

            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="update_ad",
            text=flow.get("text", ""),
            photo=flow.get("photo"),
            photo_files=flow.get("photo_files") or [],
            price=0,
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение текста отправлена на модерацию!" if created else "✅ Существующая заявка на изменение текста обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_update_photo" and attachment and attachment["type"] in {"photo", "video"} and flow.get("base_item") is not None:
        photo_value = attachment["vk_attachment"]
        new_photo_files = _cache_ad_photo_files(message, f"user_{user_id}_{int(time.time() * 1000)}", logger=logger)
        photo_files = new_photo_files
        if flow.get("append_photo"):
            photo_value = _append_photo_attachment(flow.get("photo"), photo_value)
            photo_files = merge_photo_files(flow.get("photo_files"), new_photo_files)
        _, created = _upsert_pending_edit_order(
            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="update_ad",
            text=flow.get("text", ""),
            photo=photo_value,
            photo_files=photo_files,
            price=0,
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение фото отправлена на модерацию!" if created else "✅ Существующая заявка на изменение фото обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_update_main_text" and text and not text.startswith(".") and flow.get("base_item") is not None:
        safety = detect_military_recruitment(text)
        if safety.get("blocked"):
            _reject_flow_for_safety(vk, user_id, service, ads_data, orders_data, state, safety, logger=logger)
            return
        if _contains_mass_mention(text):
            _send(
                vk,
                user_id,
                f"⚠️ Нельзя добавлять @all/@все без оплаченной опции (+{MENTION_ALL_PRICE} ₽).\nУдалите упоминание и отправьте текст снова.",
                service,
                keyboard=_kb_cancel(),
                logger=logger,
            )
            return
        flow["text"] = text
        _, created = _upsert_pending_edit_order(

            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="main_text",
            text=flow.get("text", ""),
            photo=flow.get("photo"),
            photo_files=flow.get("photo_files") or [],
            price=flow.get("price", 0),
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение текста основной рекламы отправлена на модерацию!" if created else "✅ Существующая заявка на изменение основной рекламы обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "wait_update_main_photo" and attachment and attachment["type"] in {"photo", "video"} and flow.get("base_item") is not None:
        photo_value = attachment["vk_attachment"]
        new_photo_files = _cache_ad_photo_files(message, f"user_{user_id}_{int(time.time() * 1000)}", logger=logger)
        photo_files = new_photo_files
        if flow.get("append_photo"):
            photo_value = _append_photo_attachment(flow.get("photo"), photo_value)
            photo_files = merge_photo_files(flow.get("photo_files"), new_photo_files)
        _, created = _upsert_pending_edit_order(
            service,
            orders_data,
            user_id=user_id,
            item_key=flow.get("item_key", ""),
            order_type="main_text",
            text=flow.get("text", ""),
            photo=photo_value,
            photo_files=photo_files,
            price=flow.get("price", 0),
            days=flow.get("days", 0),
            rate_name=flow.get("rate_name"),
            base_item=flow.get("base_item"),
        )
        save_orders()
        _notify_admin(vk, message_config, service, logger=logger)
        done_text = "📨 Заявка на изменение фото основной рекламы отправлена на модерацию!" if created else "✅ Существующая заявка на изменение фото основной рекламы обновлена."
        _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
        _reset_state(state)
        return

    if flow and flow.get("step") == "payment" and command == "add_check":
        flow["step"] = "wait_check"
        _send(vk, user_id, "📎 Отправьте скриншот чека или файл.", service, keyboard=_kb_cancel(), logger=logger)
        return

    if flow and flow.get("step") == "wait_check" and attachment and attachment["type"] in {"photo", "doc"}:
        flow["attachment"] = attachment
        flow["step"] = "confirm_check"
        _send(vk, user_id, "✅ Чек получен. Всё верно?", service, keyboard=_kb_check_confirm(), logger=logger)
        return

    if flow and flow.get("step") == "confirm_check":
        if command == "reupload_check":
            flow.pop("attachment", None)
            flow["step"] = "wait_check"
            _send(vk, user_id, "📎 Отправьте новый чек.", service, keyboard=_kb_cancel(), logger=logger)
            return
        if command == "submit_order":
            if bool(flow.get("mention_all")) and not _can_buy_mention_all(flow.get("days")):
                flow["mention_all"] = False
                flow["mention_all_price"] = 0
            if _contains_mass_mention(flow.get("text")) and not bool(flow.get("mention_all")):
                if _can_buy_mention_all(flow.get("days")):
                    flow["step"] = "wait_mention_all"
                    _send(
                        vk,
                        user_id,
                        f"⚠️ В тексте есть @all/@все. Для такого текста нужно купить опцию за +{MENTION_ALL_PRICE} ₽.",
                        service,
                        keyboard=_kb_mention_all(),
                        logger=logger,
                    )
                else:
                    _send(
                        vk,
                        user_id,
                        f"⚠️ В тексте есть @all/@все, но функция массового упоминания доступна только для тарифов от 30 дней.\n"
                        f"Удалите @all/@все из текста и отправьте его снова, либо выберите тариф от 30 дней.",
                        service,
                        keyboard=_kb_cancel(),
                        logger=logger,
                    )
                return
            final_text = flow.get("text", "")
            if bool(flow.get("mention_all")):
                final_text = _prepend_mass_mention(final_text)
                flow["text"] = final_text
            safety = detect_military_recruitment(
                {
                    "text": final_text,
                    "button_title": flow.get("button_title"),
                    "button_link": flow.get("button_link"),
                    "rate_name": flow.get("rate_name"),
                }
            )
            if safety.get("blocked"):
                _reject_flow_for_safety(vk, user_id, service, ads_data, orders_data, state, safety, logger=logger)
                return
            now = now_msk_str()
            attachment_payload = flow.get("attachment")
            order_id = _next_order_id(orders_data)

            order_type = "main_text" if flow.get("mode") in {"main_text", "renew_main_text"} else ("extend_ad" if flow.get("mode") == "extend" else "new_ad")
            orders_data[order_id] = {
                "order_no": _next_order_no(service, orders_data),
                "order_code": _next_order_code(service, orders_data),
                "type": order_type,
                "status": "pending",
                "user_id": user_id,
                "text": final_text,
                "photo": flow.get("photo"),
                "photo_files": flow.get("photo_files") or [],
                "price": int(flow.get("price", 0) or 0) + int(flow.get("mention_all_price", 0) or 0) + int(flow.get("custom_button_price", 0) or 0),
                "days": flow.get("days", 0),
                "rate_name": flow.get("rate_name"),
                "created_at": now,
                "attachment": attachment_payload,
                "attachment_received": now if attachment_payload else None,
                "mention_all": bool(flow.get("mention_all")),
                "mention_all_price": int(flow.get("mention_all_price", 0) or 0),
                "button_title": flow.get("button_title"),
                "button_link": flow.get("button_link"),
                "custom_button_price": int(flow.get("custom_button_price", 0) or 0),
                "events": [{"at": now, "event": "created"}],
            }
            base_item = flow.get("base_item")
            if base_item:
                orders_data[order_id]["item_key"] = base_item.get("key")
                orders_data[order_id]["source_order_id"] = base_item.get("order_id")
                orders_data[order_id]["source_order_code"] = base_item.get("order_code")
            save_orders()
            _notify_admin(vk, message_config, service, extension_only=(order_type == "extend_ad"), logger=logger)
            if order_type == "main_text":
                done_text = "📨 Заявка на покупку основной рекламы отправлена на модерацию!\nОжидайте подтверждения."
            elif order_type == "extend_ad":
                done_text = "📨 Заявка на продление отправлена на модерацию!\nОжидайте подтверждения."
            else:
                done_text = "📨 Ваша заявка отправлена на модерацию!\nОжидайте подтверждения."
            _send(vk, user_id, done_text, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
            _reset_state(state)
            return

    _send(vk, user_id, WELCOME_TEXT, service, keyboard=_main_menu(service, ads_data, orders_data, user_id), logger=logger)
