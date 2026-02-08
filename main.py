import os
import re
import time
import hmac
import hashlib
import base64
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, Response, HTTPException

APP = FastAPI()

# ===== Render Environment Variables =====
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")

UA = "UmanariLineBot/1.0 (+contact)"
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.8,en;q=0.7",
    "Connection": "close",
}

MARKS = {"◎", "○", "▲", "△", "●"}
TRACKS = ["京都", "東京", "中山", "阪神", "小倉", "福島", "新潟"]

JST = timezone(timedelta(hours=9))

# ===== Cache（汎用）=====
CACHE_TTL = 5 * 60  # seconds
_cache: Dict[str, Tuple[float, dict]] = {}  # key -> (timestamp, data)


def cache_get(key: str, ttl: int = CACHE_TTL) -> Optional[dict]:
    v = _cache.get(key)
    if not v:
        return None
    ts, data = v
    if time.time() - ts > ttl:
        _cache.pop(key, None)
        return None
    return data


def cache_set(key: str, data: dict) -> None:
    _cache[key] = (time.time(), data)


# ===== LINE署名検証 =====
def verify_line_signature(raw_body: bytes, signature: str) -> bool:
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# ===== LINE返信（最大5メッセージ）=====
def line_reply_texts(reply_token: str, texts: List[str]) -> None:
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    msgs = [{"type": "text", "text": t[:4800]} for t in texts[:5]]
    payload = {"replyToken": reply_token, "messages": msgs}
    r = requests.post(url, headers=headers, json=payload, timeout=25)
    r.raise_for_status()


def line_reply(reply_token: str, text: str) -> None:
    line_reply_texts(reply_token, [text])


# ===== 正規化 =====
def normalize_line(s: str) -> str:
    s = s.replace("\xa0", " ")     # NBSP
    s = s.replace("\u3000", " ")   # 全角スペース
    s = s.replace("，", ",")       # 全角カンマ
    s = re.sub(r"\s+", " ", s)     # 連続空白を1つに
    return s.strip()


# ===== 今日/明日 → YYYY-MM-DD =====
def resolve_day_token(tok: Optional[str]) -> Optional[str]:
    if not tok:
        return None
    tok = tok.strip()
    now = datetime.now(JST).date()
    if tok == "今日":
        return now.strftime("%Y-%m-%d")
    if tok == "明日":
        return (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return None


def is_ymd(s: str) -> bool:
    return re.match(r"^\d{4}-\d{2}-\d{2}$", s.strip()) is not None


def ymd_to_md(date_ymd: str) -> Optional[str]:
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_ymd.strip())
    if not m:
        return None
    mm = int(m.group(2))
    dd = int(m.group(3))
    return f"{mm}/{dd}"


# ===== 解析表（予想）カテゴリから、指定日の開催場を自動検出 =====
def detect_tracks_for_date(date_ymd: str) -> List[str]:
    """
    カテゴリ一覧のリンクテキストに M/D と「<場> 解析表（予想）」があるものを拾う
    """
    md = ymd_to_md(date_ymd)
    if not md:
        return []

    cache_key = f"tracks_for:{date_ymd}"
    cached = cache_get(cache_key, ttl=10 * 60)  # 10分キャッシュ
    if cached:
        return cached["tracks"]

    base = "https://www.umanari-ai.com/archives/cat_10152.html"
    pages = [base] + [f"{base}?p={i}" for i in range(2, 10)]

    found: Set[str] = set()

    for url in pages:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            t = normalize_line(a.get_text() or "")
            if md not in t:
                continue
            for tr in TRACKS:
                if (tr in t) and ("解析表（予想）" in t):
                    found.add(tr)

        # ある程度見つかったら早期終了
        if len(found) >= 3:
            break

    tracks = [tr for tr in TRACKS if tr in found]
    cache_set(cache_key, {"tracks": tracks})
    return tracks


# ===== 解析表（予想）記事URL取得（カテゴリ一覧から）=====
def find_analysis_url(track: str, date_ymd: Optional[str] = None) -> str:
    md = ymd_to_md(date_ymd) if date_ymd else None
    base = "https://www.umanari-ai.com/archives/cat_10152.html"
    pages = [base] + [f"{base}?p={i}" for i in range(2, 10)]
    key = f"{track} 解析表（予想）"

    for cat_url in pages:
        r = requests.get(cat_url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        if md:
            for a in soup.select("a"):
                t = normalize_line(a.get_text() or "")
                if (key in t) and (md in t):
                    href = a.get("href")
                    if not href:
                        continue
                    if href.startswith("http"):
                        return href
                    return requests.compat.urljoin(cat_url, href)
        else:
            for a in soup.select("a"):
                t = normalize_line(a.get_text() or "")
                if key in t:
                    href = a.get("href")
                    if not href:
                        continue
                    if href.startswith("http"):
                        return href
                    return requests.compat.urljoin(cat_url, href)

    if md:
        raise RuntimeError(f"指定日({date_ymd} / {md})の解析表URLが見つかりません: {track}")
    raise RuntimeError(f"解析表（予想）のURLが見つかりません: {track}")


def extract_marks_from_text(s: str) -> List[str]:
    return [ch for ch in s if ch in MARKS]


# ===== 記事本文パース（馬番・馬名・印が分割されても復元）=====
def parse_analysis_article(article_url: str, track: str) -> Dict[int, List[dict]]:
    r = requests.get(article_url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    node = (
        soup.select_one(".article-body")
        or soup.select_one(".articleBody")
        or soup.select_one(".entry-body")
        or soup.select_one("#article-body")
        or soup.select_one("#main")
        or soup.body
    )

    raw_text = node.get_text("\n") if node else soup.get_text("\n")
    lines = [normalize_line(x) for x in raw_text.splitlines()]
    lines = [x for x in lines if x]

    sec_re = re.compile(rf"^{re.escape(track)}\s*(\d{{1,2}})\s*R\b")

    races: Dict[int, List[dict]] = {}
    current_r: Optional[int] = None

    pending_no: Optional[int] = None
    pending_name_parts: List[str] = []
    pending_marks: List[str] = []

    def flush_pending():
        nonlocal pending_no, pending_name_parts, pending_marks
        if current_r is None:
            pending_no = None
            pending_name_parts = []
            pending_marks = []
            return
        if pending_no is None:
            return

        name = normalize_line(" ".join(pending_name_parts))
        if not name:
            pending_no = None
            pending_name_parts = []
            pending_marks = []
            return

        win = pending_marks[0] if len(pending_marks) >= 1 else ""
        bak = pending_marks[1] if len(pending_marks) >= 2 else ""
        races[current_r].append({"no": pending_no, "name": name, "win": win, "bakusou": bak})

        pending_no = None
        pending_name_parts = []
        pending_marks = []

    for line in lines:
        sm = sec_re.match(line)
        if sm:
            if current_r is not None:
                flush_pending()
            current_r = int(sm.group(1))
            races.setdefault(current_r, [])
            continue

        if current_r is None:
            continue

        if "データ不足の為解析不可" in line:
            races.pop(current_r, None)
            current_r = None
            pending_no = None
            pending_name_parts = []
            pending_marks = []
            continue

        if line == "馬名 勝率 爆走":
            continue

        if re.fullmatch(r"\d{1,2}", line):
            flush_pending()
            pending_no = int(line)
            continue

        m = re.match(r"^(\d{1,2})\s*,?\s*(.+)$", line)
        if m:
            flush_pending()
            pending_no = int(m.group(1))
            rest = m.group(2).strip()
            ms = extract_marks_from_text(rest)
            if ms:
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                rest2 = "".join(ch for ch in rest if ch not in MARKS).strip()
                if rest2:
                    pending_name_parts.append(rest2)
            else:
                pending_name_parts.append(rest)
            continue

        if pending_no is not None:
            ms = extract_marks_from_text(line)
            only_marks = normalize_line("".join(ch for ch in line if ch in MARKS))
            if ms and only_marks == normalize_line(line.replace(" ", "")):
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                continue

            if ms:
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                name_part = "".join(ch for ch in line if ch not in MARKS).strip()
                if name_part:
                    pending_name_parts.append(name_part)
                continue

            pending_name_parts.append(line)
            continue

    if current_r is not None:
        flush_pending()

    return races


def get_umanari(track: str, date_ymd: Optional[str] = None) -> dict:
    cache_key = f"umanari:{track}:{date_ymd or 'latest'}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    article_url = find_analysis_url(track, date_ymd=date_ymd)
    races = parse_analysis_article(article_url, track)
    data = {"article_url": article_url, "races": races}
    cache_set(cache_key, data)
    return data


# ===== 表整形 =====
def pad2(n: int) -> str:
    return str(n).zfill(2)


def format_race_table(rows: List[dict], mode: str = "両方") -> str:
    name_w = 12
    lines = []
    if mode == "勝率":
        lines.append("No  馬名            勝")
        lines.append("--  --------------  --")
    elif mode == "爆走":
        lines.append("No  馬名            爆")
        lines.append("--  --------------  --")
    else:
        lines.append("No  馬名            勝  爆")
        lines.append("--  --------------  --  --")

    for r in rows:
        no = pad2(r["no"])
        name = r["name"]
        if len(name) > name_w:
            name = name[: name_w - 1] + "…"
        name = name.ljust(14)

        win = (r.get("win") or "–")
        bak = (r.get("bakusou") or "–")

        if mode == "勝率":
            lines.append(f"{no}  {name}  {win}")
        elif mode == "爆走":
            lines.append(f"{no}  {name}  {bak}")
        else:
            lines.append(f"{no}  {name}  {win}   {bak}")

    return "\n".join(lines)


def build_all_races_text(track: str, date_ymd: Optional[str], article_url: str, races: Dict[int, List[dict]]) -> str:
    title = f"{track} 解析表（予想）"
    if date_ymd:
        title += f"  {date_ymd}"
    out = [title, article_url, ""]

    for rno in sorted(races.keys()):
        rows = races.get(rno, [])
        if not rows:
            continue
        out.append(f"【{track}{rno}R】")
        out.append(format_race_table(rows, mode="両方"))
        out.append("")

    if len(out) <= 3:
        out.append("該当データが見つかりませんでした（解析不可 or パースできず）。")

    return "\n".join(out).strip()


def split_for_line(text: str, max_len: int = 4400) -> List[str]:
    if len(text) <= max_len:
        return [text]
    parts, buf, buf_len = [], [], 0
    for line in text.split("\n"):
        add = line + "\n"
        if buf_len + len(add) > max_len and buf:
            parts.append("".join(buf).rstrip())
            buf, buf_len = [], 0
        buf.append(add)
        buf_len += len(add)
    if buf:
        parts.append("".join(buf).rstrip())
    return parts


# ===== 激熱抽出（勝率◎ and 爆走◎ が同一馬に付くレース）=====
def find_gekiatsu(date_ymd: str) -> dict:
    """
    return: {
      "date": date_ymd,
      "tracks": [
        {"track": "京都", "article_url": "...", "races": [
            {"race_no": 11, "horses":[{"no":2,"name":"..."}]}
        ]},
        ...
      ]
    }
    """
    cache_key = f"gekiatsu:{date_ymd}"
    cached = cache_get(cache_key, ttl=5 * 60)
    if cached:
        return cached

    tracks = detect_tracks_for_date(date_ymd)
    results = []

    for tr in tracks:
        try:
            d = get_umanari(tr, date_ymd=date_ymd)
            article_url = d["article_url"]
            races = d["races"]
        except Exception:
            continue

        hit_races = []
        for rno, rows in races.items():
            horses = []
            for r in rows:
                if (r.get("win") == "◎") and (r.get("bakusou") == "◎"):
                    horses.append({"no": r["no"], "name": r["name"]})
            if horses:
                hit_races.append({"race_no": rno, "horses": horses})

        if hit_races:
            hit_races.sort(key=lambda x: x["race_no"])
            results.append({"track": tr, "article_url": article_url, "races": hit_races})

    out = {"date": date_ymd, "tracks": results}
    cache_set(cache_key, out)
    return out


def format_gekiatsu_text(date_ymd: str) -> str:
    g = find_gekiatsu(date_ymd)
    tracks = g["tracks"]

    out = [f"激熱レース（勝率◎×爆走◎） {date_ymd}", ""]

    if not tracks:
        out.append("該当なし（勝率◎と爆走◎が同一馬に付くレースが見つかりませんでした）")
        return "\n".join(out).strip()

    for t in tracks:
        out.append(f"■ {t['track']}")
        out.append(t["article_url"])
        for rr in t["races"]:
            horses = " / ".join([f"{pad2(h['no'])} {h['name']}" for h in rr["horses"]])
            out.append(f"  {t['track']}{rr['race_no']}R：{horses}")
        out.append("")

    return "\n".join(out).strip()


# ===== コマンド解析（リッチメニューはテキスト送信前提）=====
def parse_text_command(text: str) -> Optional[dict]:
    """
    リッチメニュー推奨：
      うまなり 全 京都 今日
      うまなり 全 京都 明日
      うまなり 激熱 今日
      うまなり 激熱 明日

    互換：
      うまなり 全 京都 2026-02-08
      うまなり 京都11
      うまなり 東京5 爆走
    """
    text = text.strip()
    if not text.startswith("うまなり"):
        return None

    args = normalize_line(re.sub(r"^うまなり\s*", "", text))
    if not args:
        return {"mode": "help"}

    # 激熱： "激熱 今日/明日/YYYY-MM-DD"
    m_hot = re.match(r"^激熱\s*(今日|明日|\d{4}-\d{2}-\d{2})$", args)
    if m_hot:
        day = m_hot.group(1)
        date_ymd = resolve_day_token(day) or (day if is_ymd(day) else None)
        return {"mode": "gekiatsu", "date": date_ymd, "day_token": day}

    # 全レース： "全 京都 今日/明日/YYYY-MM-DD"
    m_all = re.match(r"^全\s*(京都|東京|中山|阪神|小倉|福島|新潟)\s*(今日|明日|\d{4}-\d{2}-\d{2})?$", args)
    if m_all:
        track = m_all.group(1)
        day = m_all.group(2)
        date_ymd = resolve_day_token(day) if day in ("今日", "明日") else (day if (day and is_ymd(day)) else None)
        return {"mode": "all", "track": track, "date": date_ymd, "day_token": day}

    # 単一R
    m_one = re.match(r"^(京都|東京|中山|阪神|小倉|福島|新潟)\s*(\d{1,2})\s*(勝率|爆走)?$", args)
    if m_one:
        return {
            "mode": "race",
            "track": m_one.group(1),
            "race": int(m_one.group(2)),
            "pick": m_one.group(3) or "両方",
        }

    return {"mode": "help"}


# ===== endpoints =====
@APP.get("/")
def health():
    return {"ok": True}


@APP.post("/webhook")
@APP.post("/webhook/")
async def webhook(req: Request):
    if not LINE_CHANNEL_SECRET or not LINE_CHANNEL_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Env vars not set")

    raw = await req.body()
    sig = req.headers.get("x-line-signature", "")
    if not verify_line_signature(raw, sig):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = await req.json()
    events = payload.get("events", [])

    for ev in events:
        if ev.get("type") != "message":
            continue

        msg = ev.get("message", {})
        if msg.get("type") != "text":
            continue

        text_in = (msg.get("text") or "").strip()
        reply_token = ev.get("replyToken")
        if not reply_token:
            continue

        cmd = parse_text_command(text_in)
        if not cmd:
            continue

        if cmd["mode"] == "help":
            line_reply(
                reply_token,
                "リッチメニュー用（おすすめ）：\n"
                "・うまなり 全 京都 今日\n"
                "・うまなり 全 京都 明日\n"
                "・うまなり 激熱 今日\n"
                "・うまなり 激熱 明日\n\n"
                "手動：\n"
                "・うまなり 全 京都 2026-02-08\n"
                "・うまなり 京都11 / うまなり 東京5 爆走"
            )
            continue

        # 激熱
        if cmd["mode"] == "gekiatsu":
            date_ymd = cmd.get("date")
            if not date_ymd:
                line_reply(reply_token, "日付が解釈できませんでした。今日/明日/YYYY-MM-DD を使ってください。")
                continue
            try:
                text = format_gekiatsu_text(date_ymd)
                chunks = split_for_line(text)
                line_reply_texts(reply_token, chunks[:5])
            except Exception as e:
                print("===== Gekiatsu ERROR START =====")
                print("date:", date_ymd)
                print("error:", repr(e))
                traceback.print_exc()
                print("===== Gekiatsu ERROR END =====")
                line_reply(reply_token, "取得に失敗しました。Render Logs を確認してください。")
            continue

        # 全R
        if cmd["mode"] == "all":
            track = cmd["track"]
            date_ymd = cmd.get("date")
            try:
                d = get_umanari(track, date_ymd=date_ymd)
                article_url = d["article_url"]
                races = d["races"]

                text = build_all_races_text(track, date_ymd, article_url, races)
                chunks = split_for_line(text)
                line_reply_texts(reply_token, chunks[:5])

            except Exception as e:
                print("===== UMANARI ALL ERROR START =====")
                print("track:", track, "date:", date_ymd)
                print("error:", repr(e))
                traceback.print_exc()
                print("===== UMANARI ALL ERROR END =====")
                line_reply(reply_token, "取得に失敗しました。Render Logs を確認してください。")
            continue

        # 単R（直近記事＝date未指定）
        if cmd["mode"] == "race":
            track = cmd["track"]
            race_no = cmd["race"]
            pick = cmd["pick"]
            try:
                d = get_umanari(track, date_ymd=None)
                article_url = d["article_url"]
                rows = d["races"].get(race_no, [])

                if not rows:
                    line_reply(reply_token, f"{track}{race_no}Rのデータが見つかりません。\n出典: {article_url}")
                    continue

                header = [f"{track}{race_no}R（出典）", article_url, ""]
                mode = pick if pick in ("勝率", "爆走") else "両方"
                table = format_race_table(rows, mode=mode)
                line_reply(reply_token, "\n".join(header) + table)

            except Exception as e:
                print("===== UMANARI RACE ERROR START =====")
                print("track:", track, "race:", race_no, "pick:", pick)
                print("error:", repr(e))
                traceback.print_exc()
                print("===== UMANARI RACE ERROR END =====")
                line_reply(reply_token, "取得に失敗しました。Render Logs を確認してください。")
            continue

    return Response(content="OK", media_type="text/plain")
