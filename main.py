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


# ---- LINE reply ----
def line_reply(reply_token: str, text: str) -> None:
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text[:4800]}],
    }
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()


# ---- normalize ----
def normalize_line(s: str) -> str:
    s = s.replace("\xa0", " ")     # NBSP
    s = s.replace("\u3000", " ")   # 全角スペース
    s = s.replace("，", ",")       # 全角カンマ
    s = re.sub(r"\s+", " ", s)     # 連続空白を1つに
    return s.strip()


# ---- find analysis url (category list) ----
def find_analysis_url(track: str) -> str:
    cat_url = "https://www.umanari-ai.com/archives/cat_10152.html"
    r = requests.get(cat_url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    key = f"{track} 解析表（予想）"
    for a in soup.select("a"):
        t = normalize_line(a.get_text() or "")
        if key in t:
            href = a.get("href")
            if not href:
                continue
            if href.startswith("http"):
                return href
            return requests.compat.urljoin(cat_url, href)

    raise RuntimeError(f"解析表（予想）のURLが見つかりません: {track}")


# ---- horse row parser (スペースなしでも対応) ----
def parse_horse_row(line: str) -> Optional[dict]:
    """
    OK例:
      "02 エムズビギン ◎ ●"
      "02エムズビギン◎●"
      "2エムズビギン◎"
      "02, エムズビギン ◎  ●"
    """
    s = normalize_line(line)
    if not s:
        return None

    # 先頭が馬番（1-2桁）で始まるものだけ
    m = re.match(r"^(\d{1,2})\s*,?\s*(.*)$", s)
    if not m:
        return None

    try:
        no = int(m.group(1))
    except ValueError:
        return None

    rest = m.group(2).strip()
    if not rest:
        return None

    # 末尾から印を最大2つはがす（スペースの有無に関係なく）
    marks = []
    while rest and rest[-1] in MARKS and len(marks) < 2:
        marks.append(rest[-1])
        rest = rest[:-1].rstrip()

    marks = list(reversed(marks))
    win = marks[0] if len(marks) >= 1 else ""
    bak = marks[1] if len(marks) >= 2 else ""

    # 馬名の末尾に残った区切り記号などを軽く除去
    name = rest.strip()
    name = re.sub(r"[|/]+$", "", name).strip()
    if not name:
        return None

    return {"no": no, "name": name, "win": win, "bakusou": bak}


# ---- parse article ----
def parse_analysis_article(article_url: str, track: str) -> Dict[int, List[dict]]:
    r = requests.get(article_url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 本文候補を優先
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

    # 11Rデバッグ用：数字で始まる行を数個だけ出す
    debug_lines_11 = []

    for line in lines:
        sm = sec_re.match(line)
        if sm:
            current_r = int(sm.group(1))
            races.setdefault(current_r, [])
            continue

        if current_r is None:
            continue

        if "データ不足の為解析不可" in line:
            races.pop(current_r, None)
            current_r = None
            continue

        if line == "馬名 勝率 爆走":
            continue

        # デバッグ収集：11R中の「数字で始まる行」を拾う（最大15行）
        if current_r == 11 and re.match(r"^\d{1,2}", line) and len(debug_lines_11) < 15:
            debug_lines_11.append(line)

        row = parse_horse_row(line)
        if row:
            races[current_r].append(row)

    # rowsが空の時に何が来てるか確認できるようにログ出し
    if 11 in races and len(races.get(11, [])) == 0:
        print("PARSE DEBUG: 11R has section but 0 horses. Sample digit-start lines:")
        for i, dl in enumerate(debug_lines_11):
            print(f"  [11R#{i}] {repr(dl)}")

    return races


def get_umanari(track: str) -> dict:
    cache_key = f"umanari:{track}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    article_url = find_analysis_url(track)
    races = parse_analysis_article(article_url, track)
    data = {"article_url": article_url, "races": races}
    cache_set(cache_key, data)
    return data


def pad2(n: int) -> str:
    return str(n).zfill(2)


def format_reply(track: str, race_no: int, mode: str, article_url: str, rows: List[dict]) -> str:
    out = [f"{track}{race_no}R（出典）", article_url, ""]

    if not rows:
        out.append("該当データが見つかりませんでした（解析不可 or パースできず）。")
        out.append("※Render Logs に 11Rのサンプル行が出るので貼ってください。")
        return "\n".join(out)

    for r in rows:
        win = r.get("win") or "–"
        bak = r.get("bakusou") or "–"
        if mode == "勝率":
            out.append(f"{pad2(r['no'])} {r['name']}  勝率:{win}")
        elif mode == "爆走":
            out.append(f"{pad2(r['no'])} {r['name']}  爆走:{bak}")
        else:
            out.append(f"{pad2(r['no'])} {r['name']}  勝率:{win} / 爆走:{bak}")

    return "\n".join(out)


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

        text = (msg.get("text") or "").strip()
        reply_token = ev.get("replyToken")
        if not reply_token:
            continue

        if not text.startswith("うまなり"):
            continue

        args = re.sub(r"^うまなり\s*", "", text).strip()
        if not args:
            line_reply(reply_token, "例）うまなり 京都11 / うまなり 東京5 爆走 / うまなり 小倉9 勝率")
            continue

        m = re.match(r"^(京都|東京|中山|阪神|小倉|福島|新潟)\s*(\d{1,2})\s*(勝率|爆走)?$", args)
        if not m:
            line_reply(reply_token, "形式：うまなり <場><R> [勝率|爆走]\n例）うまなり 京都11 爆走")
            continue

        track = m.group(1)
        race_no = int(m.group(2))
        mode = m.group(3) or "両方"

        try:
            data = get_umanari(track)
            article_url = data["article_url"]
            rows = data["races"].get(race_no, [])

            if not rows:
                print("PARSE DEBUG: empty rows",
                      "track=", track, "race=", race_no,
                      "article_url=", article_url,
                      "available_races=", sorted(list(data["races"].keys())))

            resp = format_reply(track, race_no, mode, article_url, rows)
            line_reply(reply_token, resp)

        except Exception as e:
            print("===== UMANARI ERROR START =====")
            print("track:", track, "race:", race_no, "mode:", mode)
            print("error:", repr(e))
            traceback.print_exc()
            print("===== UMANARI ERROR END =====")
            line_reply(reply_token, f"取得に失敗しました。Render Logs に原因が出ています。（{track}{race_no}R）")

    return Response(content="OK", media_type="text/plain")
