import asyncio
import logging
import os
import re
from typing import Optional
from urllib.parse import quote

import httpx
import yt_dlp
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("ethereal.api")

_API_KEY = os.getenv("API_KEY")

app = FastAPI(title="Ethereal Music API and Proxy", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

YDL_OPTS_BASE: dict = {
    "format": "bestaudio[ext=webm]/bestaudio[ext=ogg]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "extract_flat": False,
    "socket_timeout": 15,
    "http_headers": {"User-Agent": _UA},
}

if os.getenv("YTDLP_COOKIEFILE"):
    YDL_OPTS_BASE["cookiefile"] = "/etc/secrets/cookies.txt"
    # debug
    if os.path.exists("/etc/secrets/cookies.txt"):
        print("cookies.txt found, using it for authentication")
    else:
        print("cookies.txt not found, falling back to anonymous access")

CHUNK_SIZE = 1024 * 64


def _verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != _API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


def _run_ydl(url: str, opts: dict) -> dict:
    merged = {**YDL_OPTS_BASE, **opts}
    with yt_dlp.YoutubeDL(merged) as ydl:
        return ydl.extract_info(url, download=False)
        # TODO: might look into download= True later for a downloader bot mode


async def _extract_info(url: str, opts: dict | None = None) -> dict:
    loop = asyncio.get_running_loop()
    try:
        info = await loop.run_in_executor(None, _run_ydl, url, opts or {})
    except yt_dlp.utils.DownloadError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if info is None:
        raise HTTPException(status_code=404, detail="No info found for this URL.")
    return info


def _best_audio_url(info: dict) -> Optional[str]:
    if info.get("_type") == "playlist":
        entries = info.get("entries") or []
        if not entries:
            return None
        info = entries[0]

    formats: list = info.get("formats") or []

    # preferred container
    # audio only + good formats
    for fmt in reversed(formats):
        if fmt.get("vcodec") == "none" and fmt.get("acodec") not in (None, "none"):
            if fmt.get("ext") in ("webm", "ogg", "opus"):
                return fmt["url"]

    # any container
    for fmt in reversed(formats):
        if fmt.get("vcodec") == "none" and fmt.get("acodec") not in (None, "none"):
            return fmt["url"]

    # yea, we dont talk abt quality atp, and latency as well, cuz its a video
    if formats:
        return formats[-1].get("url")

    return None


def _track_metadata(info: dict) -> dict:
    return {
        "id": info.get("id"),
        "title": info.get("title"),
        "uploader": info.get("uploader") or info.get("artist") or info.get("channel"),
        "duration": info.get("duration"),
        "duration_string": info.get("duration_string"),
        "thumbnail": info.get("thumbnail"),
        "webpage_url": info.get("webpage_url") or info.get("url"),
        "extractor": info.get("extractor"),
        "upload_date": info.get("upload_date"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
    }


async def _resolve_stream(q: str) -> tuple[str, dict, dict]:
    search_query = q.strip() if re.match(r"https?://", q.strip()) else f"ytsearch1:{q}"
    log.info("Resolving: %s", search_query)

    info = await _extract_info(search_query)

    # Unwrap playlist to get first track
    if info.get("_type") == "playlist":
        entries = info.get("entries") or []
        track_info = entries[0] if entries else info
    else:
        track_info = info

    stream_url = _best_audio_url(track_info)
    if not stream_url:
        raise HTTPException(
            status_code=502, detail="Could not find a playable audio stream."
        )

    return stream_url, track_info, _track_metadata(track_info)


@app.get("/")
async def root():
    return {"status": "ok", "service": "Ethereal API", "version": "1.2.0"}


@app.get("/resolve", dependencies=[Depends(_verify_api_key)])
async def resolve(q: str):
    stream_url, info, meta = await _resolve_stream(q)
    return {**meta, "stream_url": stream_url}


@app.get("/search", dependencies=[Depends(_verify_api_key)])
async def search(
    q: str,
    limit: int = Query(5, ge=1, le=25, description="Number of results to return"),
):
    search_query = f"ytsearch{limit}:{q}"
    log.info("Searching: %s", search_query)

    info = await _extract_info(
        search_query, opts={"extract_flat": "in_playlist", "noplaylist": False}
    )
    entries = info.get("entries") or []

    results = [
        {
            "index": i + 1,
            "id": entry.get("id"),
            "title": entry.get("title"),
            "uploader": entry.get("uploader") or entry.get("channel"),
            "duration": entry.get("duration"),
            "duration_string": entry.get("duration_string"),
            "thumbnail": entry.get("thumbnail"),
            "url": entry.get("url") or entry.get("webpage_url"),
        }
        for i, entry in enumerate(entries)
    ]

    return {"query": q, "count": len(results), "results": results}


@app.get("/stream", dependencies=[Depends(_verify_api_key)])
async def stream_audio(q: str):
    stream_url, track_info, meta = await _resolve_stream(q)

    ext = track_info.get("ext") or "webm"
    # headers for httpx need it in full forms.. hence mapped
    content_type = {
        "ogg": "audio/ogg",
        "opus": "audio/ogg",
        "mp3": "audio/mpeg",
        "mp4": "audio/mp4",
        "webm": "audio/webm",
    }.get(ext, "audio/webm")

    headers = {
        "Content-Disposition": f'inline; filename="{quote(meta.get("title") or "audio")}.{ext}"',
        "X-Track-Title": meta.get("title") or "",
        "X-Track-Duration": str(meta.get("duration") or ""),
        "X-Track-Views": str(meta.get("view_count") or ""),
    }

    async def audio_generator():
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=30, write=10, pool=5),
            follow_redirects=True,
        ) as client:
            async with client.stream(
                "GET",
                stream_url,
                headers={
                    "User-Agent": _UA,
                    "Referer": "https://www.youtube.com/",
                    "Origin": "https://www.youtube.com",
                },
            ) as response:
                if response.status_code != 200:
                    log.error("Upstream %s for %s", response.status_code, stream_url)
                    return
                async for chunk in response.aiter_bytes(CHUNK_SIZE):
                    yield chunk

    return StreamingResponse(
        audio_generator(), media_type=content_type, headers=headers
    )


@app.get("/playlist/info", dependencies=[Depends(_verify_api_key)])
async def playlist_info(
    url: str,
    limit: int = Query(200, ge=1, le=500, description="Max tracks to index"),
):

    opts = {
        "extract_flat": "in_playlist",
        "noplaylist": False,
        "playlistend": limit,
        "quiet": True,
        "no_warnings": True,
    }

    info = await _extract_info(url, opts=opts)

    if info.get("_type") != "playlist":
        raise HTTPException(status_code=400, detail="niga...its not a playlist link")

    entries = (info.get("entries") or [])[:limit]

    tracks = [
        {
            "index": i + 1,
            "id": e.get("id"),
            "title": e.get("title"),
            "uploader": e.get("uploader") or e.get("channel"),
            "duration": e.get("duration"),
            "duration_string": e.get("duration_string"),
            "thumbnail": e.get("thumbnail"),
            "url": e.get("url") or e.get("webpage_url"),
        }
        for i, e in enumerate(entries)
    ]

    return {
        "playlist_title": info.get("title"),
        "playlist_uploader": info.get("uploader") or info.get("channel"),
        "playlist_url": url,
        "total": len(tracks),
        "tracks": tracks,
    }


@app.get("/playlist/track", dependencies=[Depends(_verify_api_key)])
async def playlist_track(
    url: str,
    index: int = Query(..., ge=1),
):

    opts = {
        "noplaylist": False,
        "playliststart": index,
        "playlistend": index,
        "quiet": True,
        "no_warnings": True,
    }

    info = await _extract_info(url, opts=opts)

    entries = info.get("entries") or []

    track_info = entries[0]
    stream_url = _best_audio_url(track_info)

    if not stream_url:
        raise HTTPException(
            status_code=502,
            detail=f"Could not find a playable stream for track {index}.",
        )

    meta = _track_metadata(track_info)

    return {
        **meta,
        "index": index,
        "stream_url": stream_url,
        "playlist_url": url,
    }
