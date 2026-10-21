"""Offline tests: python tests.py  (or: pytest tests.py)"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

import news_to_discord as bot

CFG = yaml.safe_load((Path(__file__).parent / "config.yaml").read_text())
NOW = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
TAX = bot.Taxonomy(CFG)


def item(title, source="BBC World", tier=1, summary="", mins_ago=30, topical=False):
    return bot.Item(id=bot.canonical_url(f"https://x.test/{abs(hash((title, source)))}"),
                    source=source, tier=tier, topical=topical, title=title,
                    summary=summary, link="https://x.test/a", time=NOW - timedelta(minutes=mins_ago))


def test_canonical_url():
    a = bot.canonical_url("https://Example.com/news/story/?utm_source=x&id=7#frag")
    assert a == "https://example.com/news/story?id=7"


def test_wildcards_and_word_boundaries():
    p = bot._compile(["palestin*", "eu"])
    assert p.search("Palestinians protest") and p.search("EU summit")
    assert not p.search("Europe weather") and not p.search("museum opens")


def test_clustering_across_outlets():
    items = [
        item("Israel and Hamas agree to new ceasefire deal in Gaza", "BBC World"),
        item("Gaza ceasefire deal agreed by Israel and Hamas", "Al Jazeera", mins_ago=20),
        item("Hamas, Israel agree Gaza ceasefire", "DW", mins_ago=10),
        item("Brazil wins football match against Argentina", "NPR World"),
    ]
    stories = bot.cluster(items, CFG["settings"]["cluster_similarity"])
    sizes = sorted(len(s.items) for s in stories)
    assert sizes == [1, 3], sizes


def test_scoring_breaking_and_relevance():
    big = bot.cluster([
        item("Missile strike kills dozens in Ukraine, Russia says", "BBC World"),
        item("Russian missile attack kills dozens in Ukraine", "Al Jazeera", mins_ago=15),
        item("Dozens killed in Russia missile attack on Ukraine", "DW", mins_ago=5),
    ], CFG["settings"]["cluster_similarity"])[0]
    bot.analyse(big, TAX, CFG, NOW)
    assert big.breaking and big.regions[0] == "europe_russia" and "conflict" in big.topics

    fluff = bot.Story([item("Local bakery wins award for best croissant")])
    bot.analyse(fluff, TAX, CFG, NOW)
    assert not fluff.relevant and not fluff.market_relevant

    analysis = bot.Story([item("Why the pivot to Asia is stalling", "Foreign Policy", 2, topical=True)])
    bot.analyse(analysis, TAX, CFG, NOW)
    assert analysis.relevant


def test_market_filter():
    def a(title):
        st = bot.Story([item(title)])
        return bot.analyse(st, TAX, CFG, NOW)

    hormuz = a("Iran threatens Strait of Hormuz closure as oil jumps")
    assert "oil_energy" in hormuz.assets and hormuz.market_relevant

    fed = a("Fed holds rates as dollar firms")           # economics, no geopolitics needed
    assert {"rates_central_banks", "fx"} <= set(fed.assets)

    indirect = a("Israeli airstrike hits Beirut suburb")  # not named, but implied
    assert not indirect.assets and "oil_energy" in indirect.indirect_assets

    local = a("Kenya holds council by-elections amid low turnout")
    assert not local.market_relevant

    gold_medal = a("Kenya wins gold medal in marathon")
    assert "gold_havens" not in gold_medal.assets


def test_routing(monkeypatch=None):
    import os
    os.environ.update(DISCORD_WEBHOOK_URL="https://d/default", DISCORD_WEBHOOK_MIDDLE_EAST="https://d/me",
                      DISCORD_WEBHOOK_BREAKING="")
    hooks = bot.Hooks(CFG)
    st = bot.Story([item("Iran talks resume in Oman")])
    bot.analyse(st, TAX, CFG, NOW)
    assert hooks.route(st) == ("middle_east", "https://d/me")
    st2 = bot.Story([item("Quarterly earnings beat estimates")])
    bot.analyse(st2, TAX, CFG, NOW)
    assert hooks.route(st2) == ("default", "https://d/default")


def test_embed_shape():
    st = bot.cluster([item("Sanctions on Iran expanded", "BBC World"),
                      item("Iran sanctions expanded by West", "DW", mins_ago=5)], 0.45)[0]
    bot.analyse(st, TAX, CFG, NOW)
    e = bot.build_embed(st, CFG)
    assert e["fields"][0]["name"] == "Also reported by" and "2 outlets" in e["footer"]["text"]


def test_dry_run_end_to_end(tmp_path=None):
    import os
    bot.STATE_PATH = Path("/tmp/_geo_state_test.json")
    bot.STATE_PATH.unlink(missing_ok=True)
    os.environ["DISCORD_WEBHOOK_URL"] = "https://d/default"
    fake = [item("Missile attack hits Kyiv as Russia escalates", "BBC World"),
            item("Russia missile attack on Kyiv escalates war", "DW", mins_ago=9),
            item("Taiwan holds military drills as China protests", "NPR World", mins_ago=50),
            item("Celebrity chef opens new restaurant", "NPR World")]
    bot.fetch_all = lambda *a, **k: fake
    chosen = bot.run(CFG, dry_run=True, now=NOW)
    assert len(chosen) == 2


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok ", name)
