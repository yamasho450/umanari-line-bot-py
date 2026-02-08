import os
import re
import time
import hmac
import hashlib
import base64
import traceback
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, Response, HTTPException

APP = FastAPI()

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

# ---- cache ----
CACHE_TTL = 5 * 60  # seconds
_cache: Dict[str, Tuple[float, dict]] = {}  # key -> (timestamp, data)


def cache_get(key: str) -> Optional[dict]:
    v = _cache.get(key)
    if not v:
        return None
    ts, data = v
    if time.time() - ts > CACHE_TTL:
        _cache.pop(key, None)
        return None
    return data


def cache_set(key: str, data: dict) -> None:
    _cache[key] = (time.time(), data)


# ---- LINE signature ----
def verify_line_signature(raw_body: bytes, signature: str) -> bool:
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# ---- LINE reply (multiple messages, up to 5) ----
def line_reply_texts(reply_token: str, texts: List[str]) -> None:
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    # LINE reply は最大5メッセージ
    msgs = [{"type": "text", "text": t[:4800]} for t in texts[:5]]
    payload = {"replyToken": reply_token, "messages": msgs}

    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()


def line_reply(reply_token: str, text: str) -> None:
    line_reply_texts(reply_token, [text])


# ---- normalize ----
def normalize_line(s: str) -> str:
    s = s.replace("\xa0", " ")     # NBSP
    s = s.replace("\u3000", " ")   # 全角スペース
    s = s.replace("，", ",")       # 全角カンマ
    s = re.sub(r"\s+", " ", s)     # 連続空白を1つに
    return s.strip()


# ---- date helper ----
def ymd_to_md(date_ymd: str) -> Optional[str]:
    """
    '2026-02-08' -> '2/8'
    変換できなければ None
    """
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_ymd.strip())
    if not m:
        return None
    mm = int(m.group(2))
    dd = int(m.group(3))
    return f"{mm}/{dd}"


# ---- find analysis url (category list, with optional date) ----
def find_analysis_url(track: str, date_ymd: Optional[str] = None) -> str:
    """
    date_ymd がある場合：リンクテキストに「M/D」と「<場> 解析表（予想）」が両方入るものを探す
    date_ymd がない場合：最新の「<場> 解析表（予想）」を取る
    """
    md = ymd_to_md(date_ymd) if date_ymd else None

    # 解析表（予想）のカテゴリ一覧。ページ送りがあるので数ページ見る
    base = "https://www.umanari-ai.com/archives/cat_10152.html"
    pages = [base] + [f"{base}?p={i}" for i in range(2, 6)]  # 最大5ページ

    key = f"{track} 解析表（予想）"

    for cat_url in pages:
        r = requests.get(cat_url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # date指定がある場合は md も含めて探す（例：2/8）
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

        # date指定がない場合：最初に見つかった最新を返す
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
    raise RuntimeError(f"解析表（予想）のURLが見つかりません: {track}"


def extract_marks_from_text(s: str) -> List[str]:
    found = [ch for ch in s if ch in MARKS]
    return found


# ---- parse analysis article (復元パース) ----
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
            # 次のRに移る前に確定
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

        # 馬番だけ行
        if re.fullmatch(r"\d{1,2}", line):
            flush_pending()
            pending_no = int(line)
            continue

        # 馬番+何か（同一行型）
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

        # pending中：馬名 or 印だけ行
        if pending_no is not None:
            ms = extract_marks_from_text(line)

            # 印だけっぽい行（記号だけ or 空白+記号）
            only_marks = normalize_line("".join(ch for ch in line if ch in MARKS))
            if ms and only_marks == normalize_line(line.replace(" ", "")):
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                continue

            # 馬名+印混在
            if ms:
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                name_part = "".join(ch for ch in line if ch not in MARKS).strip()
                if name_part:
                    pending_name_parts.append(name_part)
                continue

            # 馬名（続き）
            pending_name_parts.append(line)
            continue

        continue

    # 最後の馬を確定
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


def pad2(n: int) -> str:
    return str(n).zfill(2)


# ---- table format helpers ----
def format_race_table(rows: List[dict]) -> str:
    """
    なるべく等幅っぽく見えるように。
    ※日本語は完全には揃わないけど、LINE表示ではかなり見やすい部類。
    """
    # 馬名カラム幅（長すぎると崩れるので上限）
    name_w = 12

    lines = []
    lines.append("No  馬名            勝  爆")
    lines.append("--  --------------  --  --")
    for r in rows:
        no = pad2(r["no"])
        name = r["name"]
        if len(name) > name_w:
            name = name[: name_w - 1] + "…"
        name = name.ljust(14)  # 目安
        win = (r.get("win") or "–")
        bak = (r.get("bakusou") or "–")
        lines.append(f"{no}  {name}  {win}   {bak}")
    return "\n".join(lines)


def build_all_races_text(track: str, date_ymd: Optional[str], article_url: str, races: Dict[int, List[dict]]) -> str:
    title = f"{track} 解析表（予想）"
    if date_ymd:
        title += f"  {date_ymd}"
    out = [title, article_url, ""]

    # Rを昇順に
    for rno in sorted(races.keys()):
        rows = races.get(rno, [])
        if not rows:
            continue
        out.append(f"【{track}{rno}R】")
        out.append(format_race_table(rows))
        out.append("")  # 空行

    if len(out) <= 3:
        out.append("該当データが見つかりませんでした（解析不可 or パースできず）。")

    return "\n".join(out).strip()


def split_for_line(text: str, max_len: int = 4400) -> List[str]:
    """
    返信を複数メッセージに分割（LINE上限対策）
    目安：max_len 4400（ヘッダ等の余裕）
    """
    if len(text) <= max_len:
        return [text]

    parts = []
    buf = []
    buf_len = 0

    for line in text.split("\n"):
        add = line + "\n"
        if buf_len + len(add) > max_len and buf:
            parts.append("".join(buf).rstrip())
            buf = []
            buf_len = 0
        buf.append(add)
        buf_len += len(add)

    if buf:
        parts.append("".join(buf).rstrip())

    return parts


# ---- command parsing ----
def parse_text_command(text: str):
    """
    テキストでも操作できるように（リッチメニューがメインでも保険）
    例：
      うまなり 京都11
      うまなり 京都 2026-02-08
      うまなり 京都 2026-02-08 全
    """
    if not text.startswith("うまなり"):
        return None

    args = re.sub(r"^うまなり\s*", "", text).strip()
    if not args:
        return {"mode": "help"}

    # 全レース: "京都 2026-02-08" または "京都 2026-02-08 全"
    m_all = re.match(r"^(京都|東京|中山|阪神|小倉|福島|新潟)\s+(\d{4}-\d{2}-\d{2})(\s+全)?$", args)
    if m_all:
        return {"mode": "all", "track": m_all.group(1), "date": m_all.group(2)}

    # 単一R: "京都11" / "京都 11 爆走" / "京都11 勝率"
    m_one = re.match(r"^(京都|東京|中山|阪神|小倉|福島|新潟)\s*(\d{1,2})\s*(勝率|爆走)?$", args)
    if m_one:
        return {
            "mode": "race",
            "track": m_one.group(1),
            "race": int(m_one.group(2)),
            "pick": m_one.group(3) or "両方",
        }

    return {"mode": "help"}


def parse_postback_data(data: str) -> Optional[dict]:
    """
    Rich menu の postback.data 例：
      umanari_all|date=2026-02-08|track=京都
      umanari_all|track=京都   （date省略なら最新）
    """
    if not data:
        return None

    if not data.startswith("umanari_all"):
        return None

    # "umanari_all|k=v|k=v"
    parts = data.split("|")
    kv = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()

    track = kv.get("track")
    date = kv.get("date")  # optional

    if not track:
        return None

    return {"mode": "all", "track": track, "date": date}


# ---- endpoints ----
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
        et = ev.get("type")

        # --- postback（リッチメニュー） ---
        if et == "postback":
            reply_token = ev.get("replyToken")
            if not reply_token:
                continue

            data = (ev.get("postback", {}) or {}).get("data", "")
            cmd = parse_postback_data(data)

            if not cmd:
                line_reply(reply_token, "postback形式が不明です。data を確認してください。")
                continue

            track = cmd["track"]
            date_ymd = cmd.get("date")

            try:
                data = get_umanari(track, date_ymd=date_ymd)
                article_url = data["article_url"]
                races = data["races"]

                text = build_all_races_text(track, date_ymd, article_url, races)
                chunks = split_for_line(text)

                # LINE reply は最大5メッセージまで。超える分は切る（必要ならpushで送る設計に拡張可）
                line_reply_texts(reply_token, chunks[:5])

            except Exception as e:
                print("===== UMANARI ALL ERROR START =====")
                print("postback data:", data)
                print("track:", track, "date:", date_ymd)
                print("error:", repr(e))
                traceback.print_exc()
                print("===== UMANARI ALL ERROR END =====")
                line_reply(reply_token, "取得に失敗しました。Render Logs を確認してください。")

            continue

        # --- message（手打ちコマンド） ---
        if et == "message":
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
                    "使い方：\n"
                    "・単R：うまなり 京都11 / うまなり 東京5 爆走 / うまなり 小倉9 勝率\n"
                    "・全R：うまなり 京都 2026-02-08\n"
                    "（リッチメニューがある場合はそちら推奨）"
                )
                continue

            # 単R
            if cmd["mode"] == "race":
                track = cmd["track"]
                race_no = cmd["race"]
                pick = cmd["pick"]
                try:
                    data = get_umanari(track, date_ymd=None)
                    article_url = data["article_url"]
                    rows = data["races"].get(race_no, [])

                    if not rows:
                        line_reply(reply_token, f"{track}{race_no}Rのデータが見つかりません。\n出典: {article_url}")
                        continue

                    # 単Rは表を返す
                    header = [f"{track}{race_no}R（出典）", article_url, ""]
                    # pickでフィルタ（勝率/爆走のみ）
                    if pick == "勝率":
                        table_rows = [{**r, "bakusou": ""} for r in rows]
                    elif pick == "爆走":
                        table_rows = [{**r, "win": ""} for r in rows]
                    else:
                        table_rows = rows

                    body = format_race_table(table_rows)
                    line_reply(reply_token, "\n".join(header) + body)

                except Exception as e:
                    print("===== UMANARI RACE ERROR START =====")
                    print("track:", track, "race:", race_no, "pick:", pick)
                    print("error:", repr(e))
                    traceback.print_exc()
                    print("===== UMANARI RACE ERROR END =====")
                    line_reply(reply_token, "取得に失敗しました。Render Logs を確認してください。")
                continue

            # 全R（手打ち）
            if cmd["mode"] == "all":
                track = cmd["track"]
                date_ymd = cmd.get("date")
                try:
                    data = get_umanari(track, date_ymd=date_ymd)
                    article_url = data["article_url"]
                    races = data["races"]

                    text = build_all_races_text(track, date_ymd, article_url, races)
                    chunks = split_for_line(text)
                    line_reply_texts(reply_token, chunks[:5])

                except Exception as e:
                    print("===== UMANARI ALL(TEXT) ERROR START =====")
                    print("track:", track, "date:", date_ymd)
                    print("error:", repr(e))
                    traceback.print_exc()
                    print("===== UMANARI ALL(TEXT) ERROR END =====")
                    line_reply(reply_token, "取得に失敗しました。Render Logs を確認してください。")
                continue

    return Response(content="OK", media_type="text/plain")
