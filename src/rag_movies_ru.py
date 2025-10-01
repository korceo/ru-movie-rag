from __future__ import annotations
import os, sys, json, time, argparse
from typing import Optional, Dict, Any
from urllib.parse import quote
import requests
from dotenv import load_dotenv

load_dotenv()

TMDB_TOKEN = os.getenv("TMDB_TOKEN")          # Bearer v4
KINOPOISK_TOKEN = os.getenv("KINOPOISK_TOKEN")# X-API-KEY
UA = {"User-Agent": "ru-movie-rag/0.1 (+pixi)"}

def _get(url, *, headers=None, params=None, timeout=20) -> Optional[dict]:
    try:
        h = {} if headers is None else headers.copy()
        h.update(UA)
        r = requests.get(url, headers=h, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

# ---------- TMDB (основа) ----------
def tmdb_search_id_ru(title: str) -> Optional[int]:
    h = {"Authorization": f"Bearer {TMDB_TOKEN}"} if TMDB_TOKEN else {}
    j = _get("https://api.themoviedb.org/3/search/movie",
             headers=h, params={"query": title, "language": "ru-RU"})
    if not j or not j.get("results"): return None
    return j["results"][0]["id"]

def tmdb_movie_ru(mid: int) -> Dict[str, Any]:
    h = {"Authorization": f"Bearer {TMDB_TOKEN}"} if TMDB_TOKEN else {}
    j = _get(
        f"https://api.themoviedb.org/3/movie/{mid}",
        headers=h,
        params={
            "language": "ru-RU",
            "append_to_response": "external_ids,credits,keywords,release_dates,alternative_titles,translations,recommendations,similar"
        }
    )
    if not j:
        return {}
    # разбор credits
    crew = (j.get("credits") or {}).get("crew") or []
    cast = (j.get("credits") or {}).get("cast") or []
    directors = [p.get("name") for p in crew if (p.get("job") == "Director" and p.get("name"))]
    actors_main = [p.get("name") for p in cast if p.get("name")][:10]  # топ-10 по биллингу

    # дополнительные поля для RAG
    # writers
    writers = [p.get("name") for p in crew if p.get("job") in {"Writer","Screenplay","Story","Author"} and p.get("name")]

    # characters of top cast
    characters_main = [p.get("character") for p in cast if p.get("character")][:10]

    # alternative titles (ru/regional)
    alts = []
    for t in (j.get("alternative_titles") or {}).get("titles", []):
        if (t.get("iso_639_1") == "ru") or (t.get("iso_3166_1") in {"RU","UA","BY","KZ"}):
            n = t.get("title")
            if n and n not in alts:
                alts.append(n)

    # keywords
    _kw = (j.get("keywords") or {})
    kw_list = _kw.get("keywords") or _kw.get("results") or []
    keywords = [k.get("name") for k in kw_list if k.get("name")]

    # collection
    coll = j.get("belongs_to_collection") or {}
    collection_id = coll.get("id")
    collection_name = coll.get("name")

    # similar & recommendations (ids + titles)
    sim_results = (j.get("similar") or {}).get("results") or []
    rec_results = (j.get("recommendations") or {}).get("results") or []
    similar_tmdb_ids = [it.get("id") for it in sim_results if it.get("id")]
    recommendation_tmdb_ids = [it.get("id") for it in rec_results if it.get("id")]
    similar_titles_ru = [it.get("title") or it.get("name") for it in sim_results if (it.get("title") or it.get("name"))]
    recommendation_titles_ru = [it.get("title") or it.get("name") for it in rec_results if (it.get("title") or it.get("name"))]

    # other textual signals
    tagline = j.get("tagline")
    original_title = j.get("original_title")
    original_language = j.get("original_language")

    out = {
        "source": "tmdb",
        "tmdb_id": mid,
        "imdb_id": (j.get("external_ids") or {}).get("imdb_id"),
        "title_ru": j.get("title"),
        "overview_ru": j.get("overview") or None,
        "year": (j.get("release_date") or "")[:4] or None,
        "poster_url": ("https://image.tmdb.org/t/p/original" + j["poster_path"]) if j.get("poster_path") else None,
        "rating": j.get("vote_average"),
        "votes": j.get("vote_count"),
        "budget": j.get("budget"),
        "revenue": j.get("revenue"),
        "homepage": j.get("homepage"),
        "directors": directors,
        "actors_main": actors_main,
        "genres": [g.get("name") for g in (j.get("genres") or []) if g.get("name")],

        "writers": writers,
        "characters_main": characters_main,
        "alt_titles_ru": alts,
        "keywords": keywords,
        "original_title": original_title,
        "original_language": original_language,
        "collection_id": collection_id,
        "collection_name": collection_name,
        "tagline": tagline,
        "similar_titles_ru": similar_titles_ru[:20],
        "recommendation_titles_ru": recommendation_titles_ru[:20],
        "similar_tmdb_ids": similar_tmdb_ids,
        "recommendation_tmdb_ids": recommendation_tmdb_ids,
    }

    # если overview пуст на ru — пробуем переводы
    if not out["overview_ru"]:
        t = _get(f"https://api.themoviedb.org/3/movie/{mid}/translations", headers=h)
        if t and t.get("translations"):
            for tr in t["translations"]:
                if tr.get("iso_3166_1") == "RU" or tr.get("iso_639_1") == "ru":
                    data = tr.get("data") or {}
                    out["overview_ru"] = data.get("overview") or out["overview_ru"]
                    out["title_ru"] = data.get("title") or out["title_ru"]
                    break
    return out

def tmdb_fetch_ids_popular(n: int = 1000, language: str = "ru-RU", sleep: float = 0.25) -> list[int]:
    """Fetch up to n popular TMDB movie IDs using /movie/popular (20 per page)."""
    h = {"Authorization": f"Bearer {TMDB_TOKEN}"} if TMDB_TOKEN else {}
    ids: list[int] = []
    page = 1
    while len(ids) < n:
        j = _get(
            "https://api.themoviedb.org/3/movie/popular",
            headers=h,
            params={"language": language, "page": page},
        )
        if not j or not j.get("results"):
            break
        ids.extend([it["id"] for it in j["results"] if it.get("id")])
        total_pages = j.get("total_pages") or 1
        if page >= total_pages:
            break
        page += 1
        time.sleep(sleep)
    # de-dup while preserving order and limit to n
    seen: set[int] = set()
    uniq: list[int] = []
    for mid in ids:
        if mid in seen:
            continue
        seen.add(mid)
        uniq.append(mid)
    return uniq[:n]


def tmdb_fetch_ids_popular_range(offset: int = 0, count: int = 1000, language: str = "ru-RU", sleep: float = 0.25) -> list[int]:
    """Fetch `count` popular TMDB movie IDs starting from rank `offset` using /movie/popular (20 per page)."""
    h = {"Authorization": f"Bearer {TMDB_TOKEN}"} if TMDB_TOKEN else {}
    ids: list[int] = []
    # TMDB popular returns 20 per page
    start_page = max(1, offset // 20 + 1)
    skip_first = offset % 20
    page = start_page
    while len(ids) < count:
        j = _get(
            "https://api.themoviedb.org/3/movie/popular",
            headers=h,
            params={"language": language, "page": page},
        )
        if not j or not j.get("results"):
            break
        results = j["results"]
        if page == start_page and skip_first:
            results = results[skip_first:]
            skip_first = 0
        ids.extend([it["id"] for it in results if it.get("id")])
        total_pages = j.get("total_pages") or 1
        if page >= total_pages:
            break
        page += 1
        time.sleep(sleep)
    # de-dup, preserve order, and trim to `count`
    seen: set[int] = set()
    out: list[int] = []
    for mid in ids:
        if mid in seen:
            continue
        seen.add(mid)
        out.append(mid)
        if len(out) >= count:
            break
    return out


def get_ru_record_by_tmdb_id(mid: int, min_chars: int = 40, kp_mode: str = "missing-ru", kp_budget: list[int] | None = None) -> Dict[str, Any]:
    """Build RU record directly from a TMDB movie id, with optional KP/Wikipedia fallback."""
    rec: Dict[str, Any] = tmdb_movie_ru(mid) if TMDB_TOKEN else {}

    # kinopoisk.dev fallback if needed and within budget
    if kp_budget is None:
        kp_budget = [0]
    if kp_budget[0] > 0 and _should_use_kp(rec, min_chars, kp_mode):
        title_guess = rec.get("title_ru") or ""
        kpdoc = kp_search(title_guess) if title_guess else None
        if kpdoc:
            kp = kp_to_record(kpdoc)
            for k in ("title_ru","overview_ru","poster_url","rating","votes","year","budget","revenue","url","genres"):
                if not rec.get(k) and kp.get(k):
                    rec[k] = kp[k]
            if "kp_id" not in rec and kp.get("kp_id"):
                rec["kp_id"] = kp["kp_id"]
            kp_budget[0] -= 1

    # Wikipedia summary if RU overview is still missing/too short
    if not rec.get("overview_ru") or len(rec.get("overview_ru") or "") < min_chars:
        t = rec.get("title_ru") or ""
        if t:
            w = wiki_ru_summary(t)
            if w:
                rec["overview_ru"] = w

    # build clean output
    meta_keys = [
        "source","tmdb_id","imdb_id","kp_id","year","poster_url",
        "rating","votes","budget","revenue","homepage","url",
        "original_title","original_language","collection_id","collection_name",
        "similar_tmdb_ids","recommendation_tmdb_ids"
    ]
    meta = {k: rec.get(k) for k in meta_keys if rec.get(k) is not None}

    return {
        "title_ru": rec.get("title_ru"),
        "overview_ru": rec.get("overview_ru"),
        "directors": rec.get("directors") or [],
        "actors_main": rec.get("actors_main") or [],
        "genres": rec.get("genres") or [],
        "tagline": rec.get("tagline"),
        "alt_titles_ru": rec.get("alt_titles_ru") or [],
        "keywords": rec.get("keywords") or [],
        "characters_main": rec.get("characters_main") or [],
        "writers": rec.get("writers") or [],
        "original_title": rec.get("original_title"),
        "original_language": rec.get("original_language"),
        "collection_name": rec.get("collection_name"),
        "similar_titles_ru": rec.get("similar_titles_ru") or [],
        "recommendation_titles_ru": rec.get("recommendation_titles_ru") or [],
        "meta": meta,
    }

# ---------- kinopoisk.dev (фолбэк) ----------
def kp_search(title: str) -> Optional[dict]:
    if not KINOPOISK_TOKEN: return None
    h = {"X-API-KEY": KINOPOISK_TOKEN}
    j = _get("https://api.kinopoisk.dev/v1.4/movie/search",
             headers=h, params={"query": title, "limit": 1})
    docs = (j or {}).get("docs") or []
    return docs[0] if docs else None

def kp_to_record(doc: dict) -> Dict[str, Any]:
    return {
        "source": "kinopoisk.dev",
        "kp_id": doc.get("id"),
        "title_ru": doc.get("name") or doc.get("alternativeName"),
        "overview_ru": doc.get("description") or doc.get("shortDescription"),
        "year": doc.get("year"),
        "poster_url": ((doc.get("poster") or {}).get("url") or (doc.get("poster") or {}).get("previewUrl")),
        "rating": (doc.get("rating") or {}).get("kp"),
        "votes": (doc.get("votes") or {}).get("kp"),
        "budget": (doc.get("budget") or {}).get("value"),
        "revenue": (doc.get("fees") or {}).get("world", {}).get("value"),
        "url": f"https://www.kinopoisk.ru/film/{doc.get('id')}/" if doc.get("id") else None,
        "genres": [g.get("name") for g in (doc.get("genres") or []) if isinstance(g, dict) and g.get("name")],
    }

# ---------- Wikipedia RU (хвостовой) ----------
def wiki_ru_summary(title_ru: str) -> Optional[str]:
    j = _get(f"https://ru.wikipedia.org/api/rest_v1/page/summary/{quote(title_ru)}")
    return j.get("extract") if j else None

def _should_use_kp(rec: dict, min_chars: int, kp_mode: str) -> bool:
    overview = (rec.get("overview_ru") or "") if rec else ""
    title_ru = (rec.get("title_ru") or "") if rec else ""
    needs_ru = (not title_ru) or (not overview) or (len(overview) < min_chars)
    needs_any = needs_ru or any(not rec.get(k) for k in ("poster_url","rating","votes","budget","revenue","url"))
    if kp_mode == "never":
        return False
    if kp_mode == "always":
        return True
    if kp_mode == "missing-ru":
        return needs_ru
    if kp_mode == "missing-any":
        return needs_any
    return False

# ---------- агрегатор ----------
def get_ru_record(title: str, min_chars: int = 40, kp_mode: str = "missing-ru", kp_budget: list[int] | None = None) -> Dict[str, Any]:
    # 1) TMDB
    rec: Dict[str, Any] = {}
    mid = tmdb_search_id_ru(title) if TMDB_TOKEN else None
    if mid:
        rec = tmdb_movie_ru(mid)

    # 2) При необходимости — KP, с учётом лимита
    if kp_budget is None:
        kp_budget = [0]
    if kp_budget[0] > 0 and _should_use_kp(rec, min_chars, kp_mode):
        kpdoc = kp_search(title)
        if kpdoc:
            kp = kp_to_record(kpdoc)
            for k in ("title_ru","overview_ru","poster_url","rating","votes","year","budget","revenue","url","genres"):
                if not rec.get(k) and kp.get(k):
                    rec[k] = kp[k]
            if "kp_id" not in rec and kp.get("kp_id"):
                rec["kp_id"] = kp["kp_id"]
            kp_budget[0] -= 1  # списываем запрос к KP

    # 3) Wikipedia — только если нет внятного описания
    if not rec.get("overview_ru") or len(rec.get("overview_ru") or "") < min_chars:
        t = rec.get("title_ru") or title
        w = wiki_ru_summary(t)
        if w:
            rec["overview_ru"] = w

    # минимальный набор + чистое meta без дублей
    meta_keys = [
        "source","tmdb_id","imdb_id","kp_id","year","poster_url",
        "rating","votes","budget","revenue","homepage","url",
        "original_title","original_language","collection_id","collection_name",
        "similar_tmdb_ids","recommendation_tmdb_ids"
    ]
    meta = {k: rec.get(k) for k in meta_keys if rec.get(k) is not None}

    return {
        "title_ru": rec.get("title_ru") or title,
        "overview_ru": rec.get("overview_ru"),
        "directors": rec.get("directors") or [],
        "actors_main": rec.get("actors_main") or [],
        "genres": rec.get("genres") or [],
        "tagline": rec.get("tagline"),
        "alt_titles_ru": rec.get("alt_titles_ru") or [],
        "keywords": rec.get("keywords") or [],
        "characters_main": rec.get("characters_main") or [],
        "writers": rec.get("writers") or [],
        "original_title": rec.get("original_title"),
        "original_language": rec.get("original_language"),
        "collection_name": rec.get("collection_name"),
        "similar_titles_ru": rec.get("similar_titles_ru") or [],
        "recommendation_titles_ru": rec.get("recommendation_titles_ru") or [],
        "meta": meta
    }

# ---------- CLI ----------
def cli():
    ap = argparse.ArgumentParser(prog="ru-movie-rag")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("pull", help="Получить одну запись (JSON) по названию")
    p1.add_argument("--title", required=True, help="Название фильма (любой язык)")
    p1.add_argument("--min-chars", type=int, default=40, help="Отсечка по длине описания")
    p1.add_argument("--kp-mode", choices=["never","missing-ru","missing-any","always"], default="missing-ru", help="Когда обращаться к kinopoisk.dev: never | missing-ru | missing-any | always")
    p1.add_argument("--kp-limit", type=int, default=50, help="Максимум запросов к KP за запуск")

    p2 = sub.add_parser("batch", help="CSV -> JSONL")
    p2.add_argument("--in", dest="inp", required=True, help="Входной CSV (колонка title)")
    p2.add_argument("--out", dest="out", default="movies.jsonl", help="Выходной JSONL")
    p2.add_argument("--col", dest="col", default="title", help="Имя колонки с названиями")
    p2.add_argument("--sleep", type=float, default=0.2, help="Пауза между запросами, сек")
    p2.add_argument("--min-chars", type=int, default=40, help="Отсечка по длине описания")
    p2.add_argument("--kp-mode", choices=["never","missing-ru","missing-any","always"], default="missing-ru")
    p2.add_argument("--kp-limit", type=int, default=50)

    p3 = sub.add_parser("top", help="Выгрузить топ популярных фильмов TMDB в JSONL")
    p3.add_argument("--n", type=int, default=1000, help="Сколько фильмов взять (20 на страницу)")
    p3.add_argument("--offset", type=int, default=0, help="Сколько фильмов пропустить (смещение по рангу)")
    p3.add_argument("--out", dest="out", default="movies.jsonl", help="Куда писать JSONL")
    p3.add_argument("--sleep", type=float, default=0.25, help="Пауза между запросами, сек")
    p3.add_argument("--min-chars", type=int, default=40, help="Минимальная длина описания")
    p3.add_argument("--kp-mode", choices=["never","missing-ru","missing-any","always"], default="missing-ru")
    p3.add_argument("--kp-limit", type=int, default=50)

    args = ap.parse_args()
    if args.cmd == "pull":
        budget = [args.kp_limit]
        rec = get_ru_record(args.title, min_chars=args.min_chars, kp_mode=args.kp_mode, kp_budget=budget)
        if rec.get("overview_ru") and len(rec["overview_ru"]) < args.min_chars:
            rec["overview_ru"] = None
        print(json.dumps(rec, ensure_ascii=False))
    elif args.cmd == "batch":
        import pandas as pd
        df = pd.read_csv(args.inp)
        titles = df[args.col].astype(str).tolist()
        seen = set()
        budget = [args.kp_limit]
        with open(args.out, "w", encoding="utf-8") as f:
            for t in titles:
                key = t.strip().lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                rec = get_ru_record(t, min_chars=args.min_chars, kp_mode=args.kp_mode, kp_budget=budget)
                if rec.get("overview_ru") and len(rec["overview_ru"]) < args.min_chars:
                    rec["overview_ru"] = None
                if not rec.get("overview_ru"):
                    continue
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                time.sleep(args.sleep)
    elif args.cmd == "top":
        mids = tmdb_fetch_ids_popular_range(args.offset, args.n, sleep=args.sleep)
        budget = [args.kp_limit]
        with open(args.out, "w", encoding="utf-8") as f:
            for mid in mids:
                rec = get_ru_record_by_tmdb_id(mid, min_chars=args.min_chars, kp_mode=args.kp_mode, kp_budget=budget)
                if rec.get("overview_ru") and len(rec["overview_ru"]) < args.min_chars:
                    rec["overview_ru"] = None
                if not rec.get("overview_ru"):
                    continue
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                time.sleep(args.sleep)
    else:
        raise SystemExit("Unknown command: " + str(args.cmd))

if __name__ == "__main__":
    cli()