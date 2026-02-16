import re
from typing import List, Optional

_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "]+",
    flags=re.UNICODE,
)

_KEY_BULLETS = ("🔑", "🗝️", "🔸", "•", "-", "—", "👉", "✅", "☑️")
_NUM_PREFIX_RE = re.compile(r"^\s*(?:\(?\d{1,3}\)?\s*[\).]|\d{1,3}\s*[-—:]\s*)\s*")
_TRIM_CHARS = " \t\r\n:;,.!?()[]{}<>\"'`"

# question placeholders that mark the end of the list
_QUESTION_ONLY_RE = re.compile(r"^\s*[❓❔]+\s*$")

# additional "hard stops" (when the template has no ❓ section or it's removed)
_STOP_RE = re.compile(
    r"\b(стоимость|цена|оплат|локац|адрес|место|парковк|ставь\s*\+)\b",
    flags=re.IGNORECASE,
)

def normalize_name(name: str) -> str:
    """Normalize player name for matching/deduping (stable alias)."""
    s = name.strip()
    s = _EMOJI_RE.sub("", s)
    s = s.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"\s+", " ", s).strip()
    s = s.lstrip("@").strip(_TRIM_CHARS)
    return s.lower()


def beautify_display_name(name: str) -> str:
    """Make a name look nicer for inserts into the post.

    If the user typed a nickname fully in lowercase (latin/cyrillic), we upper-case
    the first letter we can find. We keep the rest unchanged to preserve stylistic
    choices (e.g. "black provokator" -> "Black provokator").
    """
    s = (name or "").strip()
    if not s:
        return s

    # If there is already an uppercase letter, keep as-is.
    if any(ch.isalpha() and ch.isupper() for ch in s):
        return s

    # If all cased letters are lowercase, uppercase the first alphabetical char.
    if any(ch.isalpha() for ch in s) and s.lower() == s:
        chars = list(s)
        for i, ch in enumerate(chars):
            if ch.isalpha():
                chars[i] = ch.upper()
                return "".join(chars)
    return s

def extract_event_title(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines()]
    for ln in lines[:8]:
        if not ln:
            continue
        cleaned = _EMOJI_RE.sub("", ln).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        if len(cleaned) >= 4:
            return cleaned
    return "Игровой вечер"

def _clean_player_line(raw: str) -> Optional[str]:
    s = _EMOJI_RE.sub(" ", raw).strip()

    # common bullets/markers at the beginning
    for b in _KEY_BULLETS:
        if s.startswith(b):
            s = s[len(b):].strip()

    s = re.sub(r"^[\s\-•—👉✅☑️]+", "", s).strip()

    # numbering like "1) Иван", "2. Иван", "3 - Иван", "4: Иван"
    s = _NUM_PREFIX_RE.sub("", s).strip()

    s = re.sub(r"\s+", " ", s).strip()

    # trailing +1/+2 etc
    s = re.sub(r"\s*\+\s*\d+\s*$", "", s).strip()

    return s or None

def parse_players_from_post(text: str) -> List[str]:
    """
    Parse players from the organizer post.

    Rules:
    - start after 'Список игроков:'
    - ignore empty 🔑 lines
    - ignore ❓ placeholder lines
    - IMPORTANT: once we saw the ❓ block, the very next non-empty non-❓ line
      means "list ended" -> stop parsing (prevents grabbing Location/Price/etc)
    - if ❓ block is absent, stop by next header (like 'Стоимость ...') heuristics
    """
    lines = text.splitlines()
    start_idx: Optional[int] = None
    for i, ln in enumerate(lines):
        if re.search(r"\bсписок\s+игрок", ln, flags=re.IGNORECASE):
            start_idx = i + 1
            break
    if start_idx is None:
        return []

    players: list[str] = []
    seen_questions = False

    for ln in lines[start_idx:]:
        raw = ln.strip()
        if not raw:
            # keep skipping blanks (including blanks after ❓)
            continue

        # Question placeholders: usually mark free slots. Often they come AFTER the list,
        # but in some templates they may appear BEFORE any names. We support both.
        if _QUESTION_ONLY_RE.match(raw):
            seen_questions = True
            continue

        # If we've already seen ❓ placeholders *and* we have already collected at least
        # one player, then the very next meaningful line likely belongs to the next
        # section (price/location/etc) -> stop parsing.
        #
        # If we haven't collected any players yet, keep scanning: some templates put
        # ❓ placeholders at the top of the block.
        if seen_questions and players:
            break

        # Stop when the next section header begins (e.g., "Стоимость ...", "Локация ...")
        header_candidate = _EMOJI_RE.sub("", raw).strip()
        header_candidate = re.sub(r"\s+", " ", header_candidate)
        if _STOP_RE.search(header_candidate):
            break
        if re.fullmatch(r"[A-Za-zА-Яа-яЁё0-9 _-]{1,60}:\s*", header_candidate):
            break

        cleaned = _clean_player_line(raw)
        if not cleaned:
            continue

        players.append(cleaned)

    seen = set()
    out: list[str] = []
    for p in players:
        key = normalize_name(p)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out
