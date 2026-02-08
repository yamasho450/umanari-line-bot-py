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

# ===== Render の Environment Variables に入れる =====
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
PORT = int(os.environ.get("PORT", "10000"))

UA = "UmanariLineBot/1.0 (+contact)"

HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.8,en;q=0.7",
    "Connection": "close",
}

MARKS = {"◎", "○", "▲", "△", "●"}

# ===== 簡易キャッシュ（同じ場は5分は再取得しない）=====
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


# ===== LINE署名検証 =====
def verify_line_signature(raw_body: bytes, signature: str) -> bool:
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# ===== LINE返信（Messaging API直叩き）=====
def line_reply(reply_token: str, text: str) -> None:
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text[:4800]}],  # 念のため長文カット
    }
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    r.raise_for_status()


# ===== うまなりAI：解析表（予想）カテゴリ一覧から当日っぽい記事URLを拾う =====
def find_analysis_url(track: str) -> str:
    # 解析表（予想）のカテゴリ一覧（トップより安定）
    cat_url = "https://www.umanari-ai.com/archives/cat_10152.html"
    r = requests.get(cat_url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    key = f"{track} 解析表（予想）"

    # まずはリンクテキストで探す（最優先）
    for a in soup.select("a"):
        t = (a.get_text() or "").strip()
        if key in t:
            href = a.get("href")
            if not href:
                continue
            if href.startswith("http"):
                return href
            return requests.compat.urljoin(cat_url, href)

    # 見つからない場合は、href自体に "archives/" が含まれるものを広めに拾って二次判定
    candidates = []
    for a in soup.select("a"):
        href = a.get("href") or ""
        t = (a.get_text() or "").strip()
        if "umanari-ai.com/archives/" in href or href.startswith("/archives/"):
            if track in t and "解析表（予想）" in t:
                candidates.append((t, href))

    if candidates:
        _, href = candidates[0]
        if href.startswith("http"):
            return href
        return requests.compat.urljoin(cat_url, href)

    raise RuntimeError(f"解析表（予想）のURLが見つかりません: {track}")


# ===== うまなりAI：記事本文をパースして Rごとの一覧を作る =====
def parse_analysis_article(article_url: str, track: str) -> Dict[int, List[dict]]:
    r = requests.get(article_url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 本文だけに寄せる（body全文よりノイズが減る）
    # livedoorブログは articleBody / entry-body 等のことがあるので複数候補
    node = (
        soup.select_one(".article-body")
        or soup.select_one(".articleBody")
        or soup.select_one(".entry-body")
        or soup.select_one("#article-body")
        or soup.body
    )

    text = node.get_text("\n") if node else soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # セクション例: "京都 11R きさらぎ賞" / "京都 1R"
    sec_re = re.compile(rf"^{re.escape(track)}\s+(\d{{1,2}})R\b")

    # 行例: "02 エムズビギン ◎ ●" / 印が片方だけの場合もある
    row_re = re.compile(r"^(\d{2})\s+(.+?)\s*(◎|○|▲|△|●)?\s*(◎|○|▲|△|●)?\s*$")

    races: Dict[int, List[dict]] = {}
    current_r: Optional[int] = None

    for line in lines:
        m = sec_re.match(line)
        if m:
            current_r = int(m.group(1))
            races.setdefault(current_r, [])
            continue

        if current_r is None:
            continue

        # 解析不可のRは落とす
        if "データ不足の為解析不可" in line:
            races.pop(current_r, None)
            current_r = None
            continue

        # ヘッダ行はスキップ
        if line == "馬名 勝率 爆走":
            continue

        rm = row_re.match(line)
        if rm:
            no = int(rm.group(1))
            name = rm.group(2).strip()

            # 印の出方は揺れるので、取れたものを勝率→爆走の順に入れる（片方だけでもOK）
            m1 = rm.group(3) if rm.group(3) in MARKS else ""
            m2 = rm.group(4) if rm.group(4) in MARKS else ""

            win = m1
            bak = m2

            races[current_r].append({"no": no, "name": name, "win": win, "bakusou": bak})

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
    out = []
    out.append(f"{track}{race_no}R（出典）")
    out.append(article_url)
    out.append("")

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


# ===== FastAPI endpoints =====
@APP.get("/")
def health():
    return {"ok": True}


# 307対策：末尾スラッシュあり/なし両対応
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

        # コマンド: うまなり 京都11 / うまなり 東京5 爆走 / うまなり 小倉9 勝率
        if not text.startswith("うまなり"):
            continue

        args = re.sub(r"^うまなり\s*", "", text).strip()
        if not args:
            line_reply(reply_token, "例）うまなり 京都11 / うまなり 東京5 爆走 / うまなり 小倉9 勝率")
            continue

        m = re.match(r"^(京都|東京|中山|阪神|小倉|福島|新潟)(\d{1,2})\s*(勝率|爆走)?$", args)
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
            resp = format_reply(track, race_no, mode, article_url, rows)
            line_reply(reply_token, resp)

        except Exception as e:
            # ★原因は必ずRender Logsに出す
            print("===== UMANARI ERROR START =====")
            print("track:", track, "race:", race_no, "mode:", mode)
            print("error:", repr(e))
            traceback.print_exc()
            print("===== UMANARI ERROR END =====")

            line_reply(
                reply_token,
                f"取得に失敗しました。Render Logs に原因が出ています。（{track}{race_no}R）"
            )

    return Response(content="OK", media_type="text/plain")
