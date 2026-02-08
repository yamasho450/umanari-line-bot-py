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


def extract_marks_from_text(s: str) -> List[str]:
    """文字列中に含まれる印を左→右の順で返す（最大2個を使う）"""
    found = [ch for ch in s if ch in MARKS]
    return found


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

    # セクション: "京都 11R ..." / "京都11R ..."
    sec_re = re.compile(rf"^{re.escape(track)}\s*(\d{{1,2}})\s*R\b")

    races: Dict[int, List[dict]] = {}
    current_r: Optional[int] = None

    # 状態（分割行を復元）
    pending_no: Optional[int] = None
    pending_name_parts: List[str] = []
    pending_marks: List[str] = []

    def flush_pending():
        nonlocal pending_no, pending_name_parts, pending_marks
        if pending_no is None:
            return
        name = normalize_line(" ".join(pending_name_parts))
        # nameが空なら捨てる
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

    # デバッグ：11Rで「馬番だけ行」の並びが見えるようにする
    debug_11_samples: List[str] = []

    for line in lines:
        sm = sec_re.match(line)
        if sm:
            # セクションが切り替わる前に未確定を確定
            if current_r is not None:
                flush_pending()

            current_r = int(sm.group(1))
            races.setdefault(current_r, [])
            continue

        if current_r is None:
            continue

        if "データ不足の為解析不可" in line:
            # このRは破棄
            races.pop(current_r, None)
            current_r = None
            pending_no = None
            pending_name_parts = []
            pending_marks = []
            continue

        if line == "馬名 勝率 爆走":
            continue

        # ---- ここから「分割された馬データ」を復元 ----

        # 馬番だけの行（"01" "2" など）
        if re.fullmatch(r"\d{1,2}", line):
            if current_r == 11 and len(debug_11_samples) < 15:
                debug_11_samples.append(line)

            # 次の馬番が来た＝前の馬を確定してから新規開始
            flush_pending()
            pending_no = int(line)
            continue

        # 馬番＋何か（"01 カシノ..." / "01,カシノ..." みたいなケースも拾う）
        m = re.match(r"^(\d{1,2})\s*,?\s*(.+)$", line)
        if m:
            flush_pending()
            pending_no = int(m.group(1))
            rest = m.group(2).strip()
            # 同じ行に印も入ってたら取る
            ms = extract_marks_from_text(rest)
            if ms:
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                # 印を除去した残りを馬名候補へ
                rest2 = "".join(ch for ch in rest if ch not in MARKS).strip()
                if rest2:
                    pending_name_parts.append(rest2)
            else:
                pending_name_parts.append(rest)
            continue

        # ここに来るのは「馬番の次の行」＝馬名 or 印だけ or 続き
        if pending_no is not None:
            ms = extract_marks_from_text(line)

            # 印だけの行（例："◎" や "◎ ●"）
            if ms and normalize_line("".join(ch for ch in line if ch in MARKS)) == normalize_line(line.replace(" ", "")):
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                continue

            # 印が混ざっている行（馬名 + 印）もあるので分離
            if ms:
                pending_marks.extend(ms[: 2 - len(pending_marks)])
                name_part = "".join(ch for ch in line if ch not in MARKS).strip()
                if name_part:
                    pending_name_parts.append(name_part)
                continue

            # ただの馬名（または馬名の続き）
            pending_name_parts.append(line)
            continue

        # pending_no が無いのにここに来た行は無視
        continue

    # 最後の馬を確定
    if current_r is not None:
        flush_pending()

    # まだ11Rが空なら、ログに「馬番だけ行」サンプルを出す
    if 11 in races and len(races.get(11, [])) == 0:
        print("PARSE DEBUG: 11R has section but 0 horses. Sample number-only lines:", debug_11_samples)

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
