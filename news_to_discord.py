"""
Geopolitical news -> Discord.

Pipeline:  fetch RSS (parallel) -> normalise -> drop already-seen -> cluster the same
story across outlets -> tag region/topic -> score -> select -> route to channels -> post.

Usage:
    python news_to_discord.py              # normal run (post new stories)
    python news_to_discord.py --digest     # post the daily briefing of the last 24h
    python news_to_discord.py --dry-run    # print what WOULD be posted; sends/saves nothing
"""
from __future__ import annotations

import argparse
import calendar
import html
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser
import requests
import yaml

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.yaml"
STATE_PATH = ROOT / "state.json"
UA = "geo-news-discord-bot/2.0 (+rss reader)"
RED = 0xE53E3E
GREY = 0x718096
log = logging.getLogger("geo-news")

# ----------------------------------------------------------------------------- text utils

TRACKING_PARAMS = {"fbclid", "gclid", "ocid", "cmpid", "ref", "ref_src", "at_medium",
                   "at_campaign", "maxwell_id"}
STOPWORDS = set("""a an the and or but of in on at to for from by with as is are was were be been
has have had it its this that these those after over into amid says said say will would could
new about against more than their his her they who what when where how why not no""".split())


def clean(text: str, limit: int) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = re.sub(r"\s+", " ", html.unescape(text)).strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def canonical_url(url: str) -> str:
    p = urlsplit(url.strip())
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if not k.lower().startswith("utm_") and k.lower() not in TRACKING_PARAMS]
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/") or "/",
                       urlencode(q), ""))


def tokens(title: str) -> frozenset[str]:
    words = re.findall(r"[a-z0-9']+", title.lower().replace("’", "'"))
    return frozenset(w.removesuffix("'s") for w in words if len(w) > 2 and w not in STOPWORDS)


def similar(a: frozenset, b: frozenset, threshold: float) -> bool:
    inter = len(a & b)
    if not inter:
        return False
    jaccard = inter / len(a | b)
    overlap = inter / min(len(a), len(b))
    return jaccard >= threshold or (inter >= 3 and overlap >= 0.7)


def hex_color(value: str) -> int:
    return int(str(value).lstrip("#"), 16)


# ----------------------------------------------------------------------------- data model

@dataclass
class Item:
    id: str
    source: str
    tier: int
    topical: bool
    title: str
    summary: str
    link: str
    time: datetime
    image: str | None = None

    @property
    def tokens(self) -> frozenset[str]:
        return tokens(self.title)


@dataclass
class Story:
    items: list[Item]
    regions: list[str] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    assets: list[str] = field(default_factory=list)           # market areas named in the text
    indirect_assets: list[str] = field(default_factory=list)  # inferred from region + topic rules
    urgency: int = 0
    score: float = 0.0
    breaking: bool = False
    relevant: bool = True
    blurb: str | None = None

    @property
    def lead(self) -> Item:
        return min(self.items, key=lambda i: (i.tier, i.time))

    @property
    def sources(self) -> list[str]:
        seen, out = set(), []
        for i in sorted(self.items, key=lambda i: (i.tier, i.time)):
            if i.source not in seen:
                seen.add(i.source)
                out.append(i.source)
        return out

    @property
    def newest(self) -> datetime:
        return max(i.time for i in self.items)

    @property
    def market_relevant(self) -> bool:
        return bool(self.assets or self.indirect_assets)


# ----------------------------------------------------------------------------- taxonomy / scoring

def _compile(keywords: list[str]) -> re.Pattern:
    parts = []
    for k in keywords:
        k = k.strip().lower()
        parts.append(re.escape(k[:-1]) + r"\w*" if k.endswith("*") else re.escape(k))
    return re.compile(r"\b(?:" + "|".join(parts) + r")\b", re.I)


def _count(pattern: re.Pattern, text: str) -> int:
    return len({m.lower() for m in pattern.findall(text)})


class Taxonomy:
    def __init__(self, cfg: dict):
        self.regions = {n: _compile(v["keywords"]) for n, v in cfg["regions"].items()}
        self.topics = {n: _compile(v["keywords"]) for n, v in cfg["topics"].items()}
        self.urgency = _compile(cfg["urgency"])
        self.assets = {n: _compile(v["keywords"]) for n, v in (cfg.get("markets") or {}).items()}
        self.implied = cfg.get("implied_markets") or []


def analyse(story: Story, tax: Taxonomy, cfg: dict, now: datetime) -> Story:
    s = cfg["settings"]
    text = " ".join(f"{i.title} {i.summary}" for i in story.items)

    counts = {n: _count(p, text) for n, p in tax.regions.items()}
    story.regions = [n for n, c in sorted(counts.items(), key=lambda kv: -kv[1]) if c]
    story.topics = [n for n, p in tax.topics.items() if p.search(text)]
    story.urgency = _count(tax.urgency, text)

    # Market impact: assets named explicitly, plus ones implied by region + topic rules.
    acounts = {n: _count(p, text) for n, p in tax.assets.items()}
    story.assets = [n for n, c in sorted(acounts.items(), key=lambda kv: -kv[1]) if c]
    story.indirect_assets = []
    for rule in tax.implied:
        if not (rule.get("regions") or rule.get("topics")):
            continue
        r_ok = not rule.get("regions") or any(r in story.regions for r in rule["regions"])
        t_ok = not rule.get("topics") or any(t in story.topics for t in rule["topics"])
        if r_ok and t_ok:
            for a in rule["assets"]:
                if a not in story.assets and a not in story.indirect_assets:
                    story.indirect_assets.append(a)

    topical = any(i.topical for i in story.items)
    story.relevant = bool(story.regions or story.topics or story.urgency or topical)

    lead = story.lead
    tier_w = {1: 3.0, 2: 2.0}.get(lead.tier, 1.0)
    coverage = min(1.5 * (len(story.sources) - 1), 4.5)
    topic_w = min(0.5 * len(story.topics), 1.5)
    urgency_w = min(1.5 * story.urgency, 4.5)
    age_h = max((now - story.newest).total_seconds() / 3600, 0)
    recency = max(0.0, 1 - age_h / s["max_age_hours"])
    market_w = min(1.0 * len(story.assets), 3.0) + min(0.5 * len(story.indirect_assets), 1.0)
    story.score = round(tier_w + coverage + topic_w + urgency_w + recency + market_w
                        + (1.0 if topical else 0), 2)
    story.breaking = story.urgency >= 1 and story.score >= s["breaking_score"]
    return story


def cluster(items: list[Item], threshold: float) -> list[Story]:
    """Greedy clustering: items whose headlines overlap enough become one story."""
    stories: list[Story] = []
    for item in sorted(items, key=lambda i: i.time):
        for st in stories:
            if any(similar(item.tokens, m.tokens, threshold) for m in st.items):
                st.items.append(item)
                break
        else:
            stories.append(Story(items=[item]))
    return stories


# ----------------------------------------------------------------------------- fetching

def _image(entry) -> str | None:
    for key in ("media_thumbnail", "media_content"):
        for m in entry.get(key, []) or []:
            url = m.get("url")
            if url and (key == "media_thumbnail" or "image" in (m.get("type") or "image")):
                return url
    for link in entry.get("links", []) or []:
        if (link.get("type") or "").startswith("image/"):
            return link.get("href")
    return None


def fetch_source(src: dict, cutoff: datetime, now: datetime) -> list[Item]:
    try:
        r = requests.get(src["url"], headers={"User-Agent": UA}, timeout=20)
        r.raise_for_status()
        feed = feedparser.parse(r.content)
    except Exception as exc:
        log.warning("%s: fetch failed (%s)", src["name"], exc)
        return []
    if feed.bozo and not feed.entries:
        log.warning("%s: feed could not be parsed", src["name"])
        return []

    out = []
    for e in feed.entries:
        link = e.get("link")
        if not link:
            continue
        t = e.get("published_parsed") or e.get("updated_parsed")
        when = datetime.fromtimestamp(calendar.timegm(t), tz=timezone.utc) if t else now
        if when < cutoff or when > now + timedelta(hours=1):
            continue
        title = clean(e.get("title", ""), 256)
        if not title:
            continue
        out.append(Item(
            id=canonical_url(link), source=src["name"], tier=src.get("tier", 3),
            topical=bool(src.get("topical")), title=title,
            summary=clean(e.get("summary", ""), 400), link=link, time=when, image=_image(e),
        ))
    log.info("%s: %d recent item(s)", src["name"], len(out))
    return out


def fetch_all(sources: list[dict], cutoff: datetime, now: datetime) -> list[Item]:
    with ThreadPoolExecutor(max_workers=8) as pool:
        batches = pool.map(lambda s: fetch_source(s, cutoff, now), sources)
    unique: dict[str, Item] = {}
    for batch in batches:
        for item in batch:
            unique.setdefault(item.id, item)
    return list(unique.values())


# ----------------------------------------------------------------------------- state

def load_state() -> tuple[dict, bool]:
    if STATE_PATH.exists():
        try:
            st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            st.setdefault("seen", {})
            st.setdefault("posted", [])
            return st, False
        except json.JSONDecodeError:
            log.error("state.json is corrupt; starting fresh")
    return {"seen": {}, "posted": []}, True


def prune_state(state: dict, now: datetime, days: int) -> None:
    cutoff = (now - timedelta(days=days)).isoformat()
    state["seen"] = {k: v for k, v in state["seen"].items() if v >= cutoff}
    state["posted"] = [p for p in state["posted"] if p["time"] >= cutoff]


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")


# ----------------------------------------------------------------------------- discord

class Hooks:
    """Resolves webhook URLs from environment variables named in config."""

    def __init__(self, cfg: dict):
        ch = cfg.get("channels", {})
        get = lambda name: (os.environ.get(name or "") or "").strip() or None  # noqa: E731
        self.default = get(ch.get("default", "DISCORD_WEBHOOK_URL"))
        self.breaking = get(ch.get("breaking"))
        self.digest = get(ch.get("digest"))
        self.regions = {r: get(v) for r, v in (ch.get("regions") or {}).items()}

    def route(self, story: Story) -> tuple[str, str | None]:
        """Return (channel_label, url)."""
        if story.breaking and self.breaking:
            return "breaking", self.breaking
        if story.regions and self.regions.get(story.regions[0]):
            return story.regions[0], self.regions[story.regions[0]]
        return "default", self.default


def send(url: str, embeds: list[dict], content: str | None = None, mention: bool = False) -> None:
    body = {"embeds": embeds,
            # Never let a headline containing "@everyone" ping anyone.
            "allowed_mentions": {"parse": ["everyone"] if mention else []}}
    if content:
        body["content"] = content
    for _ in range(5):
        r = requests.post(url, json=body, timeout=20)
        if r.status_code == 429:
            time.sleep(float(r.json().get("retry_after", 2)) + 0.5)
            continue
        r.raise_for_status()
        return
    raise RuntimeError("Discord rate limit: gave up after retries")


def build_embed(story: Story, cfg: dict) -> dict:
    lead = story.lead
    primary = story.regions[0] if story.regions else None
    color = RED if story.breaking else (hex_color(cfg["regions"][primary]["color"]) if primary else GREY)

    tags = [cfg["regions"][r]["label"] for r in story.regions[:2]]
    tags += [cfg["topics"][t]["label"] for t in story.topics[:3]]

    embed = {
        "title": lead.title[:256],
        "url": lead.link,
        "description": (story.blurb or lead.summary)[:600],
        "color": color,
        "timestamp": story.newest.isoformat(),
        "footer": {"text": lead.source + (f" · {len(story.sources)} outlets" if len(story.sources) > 1 else "")},
    }
    if tags:
        embed["author"] = {"name": " · ".join(tags)[:256]}

    others, seen = [], {lead.source}
    for i in sorted(story.items, key=lambda i: (i.tier, i.time)):
        if i.source not in seen:
            seen.add(i.source)
            others.append(f"[{i.source}]({i.link})")
    if others:
        embed["fields"] = [{"name": "Also reported by", "value": " · ".join(others)[:1024]}]

    if story.market_relevant:
        m = cfg.get("markets", {})
        label = lambda a: f"{m.get(a, {}).get('emoji', '')} {m.get(a, {}).get('label', a)}".strip()  # noqa: E731
        names = [label(a) for a in story.assets[:4]]
        names += [f"{label(a)} (indirect)" for a in story.indirect_assets[:3]]
        embed.setdefault("fields", []).append(
            {"name": "Markets in focus", "value": " · ".join(names)[:1024]})

    image = lead.image or next((i.image for i in story.items if i.image), None)
    if image:
        embed["thumbnail"] = {"url": image}
    return embed


def ai_summary(story: Story, cfg: dict) -> str | None:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    material = "\n".join(f"- {i.source}: {i.title}. {i.summary}" for i in story.items[:4])
    payload = {
        "model": os.environ.get("ANTHROPIC_MODEL") or cfg["settings"].get("ai_model", "claude-haiku-5-5"),
        "max_tokens": 120,
        "system": ("You write neutral one-sentence news briefs. Use ONLY facts stated in the "
                   "provided headlines/snippets. Do not add context, speculation, or details "
                   "that are not present. Maximum 35 words. Output the sentence only."),
        "messages": [{"role": "user", "content": material}],
    }
    try:
        r = requests.post("https://api.anthropic.com/v1/messages", timeout=30, json=payload,
                          headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                                   "content-type": "application/json"})
        r.raise_for_status()
        text = "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
        return text.strip() or None
    except Exception as exc:
        log.warning("AI summary failed (%s); using feed snippet", exc)
        return None


def deliver(url: str | None, label: str, stories: list[Story], cfg: dict, dry_run: bool) -> list[Story]:
    """Post stories to one webhook. Breaking stories get their own message. Returns those sent."""
    mention = cfg["settings"].get("breaking_mention", "").strip()
    messages: list[tuple[str | None, list[Story]]] = []
    batch: list[Story] = []
    for st in stories:
        if st.breaking:
            messages.append((f"🚨 **BREAKING** {mention}".strip(), [st]))
        else:
            batch.append(st)
            if len(batch) == 4:
                messages.append((None, batch))
                batch = []
    if batch:
        messages.append((None, batch))

    sent: list[Story] = []
    for content, group in messages:
        embeds = [build_embed(s, cfg) for s in group]
        if dry_run:
            for s, e in zip(group, embeds):
                flag = "BREAKING " if s.breaking else ""
                print(f"[{label}] {flag}score={s.score} | {e.get('author', {}).get('name', '-')}\n"
                      f"    {e['title']}\n    {e['footer']['text']}  <{s.lead.link}>")
            sent += group
            continue
        try:
            send(url, embeds, content, mention=bool(mention) and "@" in (content or ""))
            sent += group
            time.sleep(1)
        except Exception as exc:
            log.error("Failed to post to %s: %s", label, exc)
    return sent


# ----------------------------------------------------------------------------- main flows

def run(cfg: dict, dry_run: bool = False, now: datetime | None = None) -> list[Story]:
    s = cfg["settings"]
    now = now or datetime.now(timezone.utc)
    hooks = Hooks(cfg)
    if not dry_run and not hooks.default:
        sys.exit("Set the DISCORD_WEBHOOK_URL environment variable (default channel webhook).")

    state, first_run = load_state()
    prune_state(state, now, s["state_retention_days"])
    tax = Taxonomy(cfg)

    items = fetch_all(cfg["sources"], now - timedelta(hours=s["max_age_hours"]), now)
    fresh = [i for i in items if i.id not in state["seen"]]
    log.info("%d fetched, %d not yet seen", len(items), len(fresh))

    dup_cutoff = (now - timedelta(hours=s["dup_window_hours"])).isoformat()
    recent = [tokens(p["title"]) for p in state["posted"] if p["time"] >= dup_cutoff]
    stamp = now.isoformat()

    candidates: list[Story] = []
    for story in cluster(fresh, s["cluster_similarity"]):
        analyse(story, tax, cfg, now)
        already_posted = any(similar(story.lead.tokens, t, s["cluster_similarity"]) for t in recent)
        if s.get("market_filter"):
            irrelevant = not story.market_relevant
        else:
            irrelevant = s["require_relevance"] and not story.relevant
        if already_posted or irrelevant or story.score < s["min_score"]:
            for i in story.items:
                state["seen"][i.id] = stamp
            continue
        candidates.append(story)

    candidates.sort(key=lambda st: st.score, reverse=True)
    limit = s["first_run_posts"] if first_run else s["max_posts_per_run"]
    chosen = candidates[:limit]
    if first_run:  # don't let the backlog dribble out after the first run
        for st in candidates[limit:]:
            for i in st.items:
                state["seen"][i.id] = stamp
    # Un-chosen candidates stay unseen: they compete again next run until they age out.

    if s.get("ai_summaries"):
        for st in chosen:
            st.blurb = ai_summary(st, cfg)

    chosen.sort(key=lambda st: st.newest)  # chronological in the channel
    by_hook: dict[tuple[str, str | None], list[Story]] = defaultdict(list)
    for st in chosen:
        by_hook[hooks.route(st)].append(st)

    posted_total = 0
    for (label, url), stories in by_hook.items():
        for st in deliver(url, label, stories, cfg, dry_run):
            posted_total += 1
            for i in st.items:
                state["seen"][i.id] = stamp
            state["posted"].append({
                "title": st.lead.title, "link": st.lead.link, "time": stamp,
                "score": st.score, "sources": st.sources,
                "regions": [cfg["regions"][r]["label"] for r in st.regions[:2]],
                "topics": [cfg["topics"][t]["label"] for t in st.topics[:2]],
                "assets": [cfg.get("markets", {}).get(a, {}).get("label", a)
                           for a in (st.assets + st.indirect_assets)[:3]],
            })

    log.info("Posted %d story(ies); %d candidate(s) total", posted_total, len(candidates))
    if not dry_run:
        save_state(state)
    return chosen


def md_escape(text: str) -> str:
    return text.replace("[", "(").replace("]", ")")


def digest(cfg: dict, dry_run: bool = False, now: datetime | None = None) -> None:
    s = cfg["settings"]
    now = now or datetime.now(timezone.utc)
    hooks = Hooks(cfg)
    url = hooks.digest or hooks.default
    if not dry_run and not url:
        sys.exit("Set DISCORD_WEBHOOK_URL (or the digest webhook).")

    state, _ = load_state()
    cutoff = (now - timedelta(hours=24)).isoformat()
    top = sorted((p for p in state["posted"] if p["time"] >= cutoff),
                 key=lambda p: p["score"], reverse=True)[: s["digest_size"]]
    if not top:
        log.info("Nothing posted in the last 24h; no digest.")
        return

    lines = []
    for n, p in enumerate(top, 1):
        tags = " · ".join(p.get("regions", [])[:1] + p.get("assets", []))
        lines.append(f"**{n}.** [{md_escape(p['title'])}]({p['link']})\n"
                     f"└ {tags + ' — ' if tags else ''}{', '.join(p['sources'][:3])}")
    local = now + timedelta(minutes=s["utc_offset_minutes"])
    embed = {"title": f"🗞️ Daily Market-Moving News Briefing · {local:%d %b %Y}",
             "description": "\n".join(lines)[:4000], "color": 0x2B6CB0,
             "footer": {"text": f"Top {len(top)} stories from the last 24 hours, ranked by coverage and significance · Not financial advice"}}
    if dry_run:
        print(embed["title"], "\n", embed["description"])
    else:
        send(url, [embed])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--digest", action="store_true", help="post the daily briefing")
    ap.add_argument("--dry-run", action="store_true", help="print instead of posting; saves no state")
    ap.add_argument("--config", type=Path, default=CONFIG_PATH)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    cfg = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    (digest if args.digest else run)(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
