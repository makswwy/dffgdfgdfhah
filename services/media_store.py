from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Iterable
from urllib.request import urlopen


def split_attachment_refs(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_parts = value
    else:
        raw_parts = str(value or "").split(",")
    return [str(part).strip() for part in raw_parts if str(part).strip()]


def normalize_photo_files(value: Any) -> list[str | None]:
    if isinstance(value, list):
        return [str(item).strip() if item else None for item in value]
    if isinstance(value, str) and value.strip():
        return [part.strip() or None for part in value.split(",")]
    return []


def merge_photo_files(existing: Any, new_files: Iterable[str | None]) -> list[str | None]:
    merged = normalize_photo_files(existing)
    merged.extend([str(item).strip() if item else None for item in new_files])
    return merged


def best_photo_url(photo: dict[str, Any] | None) -> str | None:
    if not isinstance(photo, dict):
        return None
    sizes = photo.get("sizes") or []
    if not isinstance(sizes, list) or not sizes:
        return None
    best = max(sizes, key=lambda item: int(item.get("width", 0) or 0) * int(item.get("height", 0) or 0))
    url = best.get("url")
    return str(url) if url else None


def _safe_name(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return clean[:80] or "photo"


def _relative_to_base(base_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        return path.as_posix()


def save_photo_from_url(
    base_dir: str | Path,
    url: str | None,
    *,
    prefix: str,
    index: int = 0,
    logger: Any = None,
) -> str | None:
    if not url:
        return None

    base_path = Path(base_dir)
    target_dir = base_path / "photos" / "ads"
    target_dir.mkdir(parents=True, exist_ok=True)

    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    target = target_dir / f"{_safe_name(prefix)}_{index}_{digest}.jpg"
    if target.exists() and target.stat().st_size > 0:
        return _relative_to_base(base_path, target)

    try:
        with urlopen(url, timeout=30) as response:
            target.write_bytes(response.read())
    except Exception as exc:
        if logger:
            logger.warning(f"Could not cache VK photo locally: {exc}")
        return None

    return _relative_to_base(base_path, target)


def save_photo_payload(
    base_dir: str | Path,
    photo: dict[str, Any] | None,
    *,
    prefix: str,
    index: int = 0,
    logger: Any = None,
) -> str | None:
    return save_photo_from_url(base_dir, best_photo_url(photo), prefix=prefix, index=index, logger=logger)


def save_photo_files_from_attachments(
    base_dir: str | Path,
    attachments: Iterable[dict[str, Any]],
    *,
    prefix: str,
    allowed_kinds: set[str] | None = None,
    logger: Any = None,
) -> list[str | None]:
    allowed = allowed_kinds or {"photo", "video"}
    files: list[str | None] = []
    saved_index = 0
    for raw in attachments:
        kind = raw.get("type")
        if kind not in allowed:
            continue
        if kind != "photo":
            files.append(None)
            continue
        files.append(save_photo_payload(base_dir, raw.get("photo"), prefix=prefix, index=saved_index, logger=logger))
        saved_index += 1
    return files
