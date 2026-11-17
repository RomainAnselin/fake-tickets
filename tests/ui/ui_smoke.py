"""Browser smoke test of the UI (not collected by pytest).

Prereqs: a running frontend in FT_AUTH_MODE=local with admin / admin-pass-123, its backend,
and a Cassandra at 127.0.0.1:19042.  pip install playwright; then:

    python tests/ui/ui_smoke.py <workdir> [chromium-path]

<workdir> must contain scbzip/secure-connect-demo.zip (any zip with a config.json).
"""
import re, sys, time
from playwright.sync_api import sync_playwright, expect

S = sys.argv[1]
CHROMIUM = sys.argv[2] if len(sys.argv) > 2 else "/usr/bin/chromium-browser"
U = "https://localhost:18443/"
problems = []
with sync_playwright() as p:
    b = p.chromium.launch(executable_path=CHROMIUM, args=["--no-sandbox"])
    pg = b.new_page(ignore_https_errors=True, viewport={"width": 1400, "height": 1000})
    pg.on("pageerror", lambda e: problems.append(f"pageerror: {e}"))
    pg.on("console", lambda m: m.type == "error" and problems.append(f"console: {m.text}"))
    pg.goto(U)
    # login
    expect(pg.locator("#login-view")).to_be_visible()
    pg.fill("input[name=username]", "admin"); pg.fill("input[name=password]", "wrong")
    pg.click("#login-form button[type=submit]")
    expect(pg.locator("#login-error")).to_contain_text("invalid")
    pg.fill("input[name=password]", "admin-pass-123"); pg.click("#login-form button[type=submit]")
    expect(pg.locator("#app-view")).to_be_visible(); print("login ok")
    # new connection
    pg.click("#new-conn")
    f = pg.locator("#conn-form")
    f.locator("input[name=name]").fill("ui-lab")
    f.locator("input[name=contact_points]").fill("127.0.0.1")
    f.locator("input[name=port]").fill("19042")
    f.locator("input[name=replication_factor]").fill("1")
    f.locator("input[name=password]").fill("ui-secret")
    f.locator("input[name=keyspace]").fill("ft_ui")
    pg.click("#conn-save")
    expect(pg.locator("#conn-list")).to_contain_text("ui-lab"); print("connection created")
    # test connectivity
    pg.click("#test-conn")
    expect(pg.locator("#run-detail")).to_contain_text("Connectivity test", timeout=15000)
    expect(pg.locator("#run-detail .json")).to_contain_text("release_version", timeout=20000); print("connectivity ok")
    # start a write run at 1k/s
    pg.check("#wl-list input[value=write]", force=True)
    pg.fill("#p-table", "ui_t")
    pg.locator("#rate-control button.pill", has_text="1k/s").click()
    pg.click("#start-run")
    expect(pg.locator("#run-detail")).to_contain_text("Write fake tickets", timeout=15000)
    expect(pg.locator("#run-detail .state").first).to_have_text("running", timeout=20000)
    time.sleep(4)
    rate_now = pg.locator("#run-detail .tile").nth(0).inner_text(); print("rate tile:", rate_now.replace("\n", " "))
    # live: preset 5k/s then slider
    pg.locator("#rd-live button.pill", has_text="5k/s").click()
    expect(pg.locator("#toast")).to_contain_text("5.0k", timeout=5000)
    time.sleep(4)
    print("after preset:", pg.locator("#run-detail .tile").nth(0).inner_text().replace("\n", " "), "|", pg.locator("#run-detail .tile").nth(1).inner_text().replace("\n", " "))
    pg.locator("#rd-live input[type=range]").first.fill("600")  # log scale: 600/1000 of 10^5 → 1 000 ops/s
    time.sleep(4)
    tgt = pg.locator("#run-detail .tile").nth(1).inner_text().replace("\n", " "); print("after slider:", tgt)
    assert "5.0k" not in tgt, "slider did not change target"
    pg.screenshot(path=f"{S}/ui-driven.png", full_page=True)
    # stop
    pg.locator("#run-detail button", has_text="Stop").click()
    expect(pg.locator("#run-detail .state").first).to_have_text("stopped", timeout=30000); print("stopped ok")
    # edit connection (password kept)
    pg.click("#edit-conn")
    expect(f.locator("input[name=password]")).to_have_attribute("placeholder", re.compile("unchanged"))
    f.locator("input[name=name]").fill("ui-lab-renamed"); pg.click("#conn-save")
    expect(pg.locator("#conn-list")).to_contain_text("ui-lab-renamed"); print("edit ok")
    # astra with SCB upload
    pg.click("#new-conn")
    f.locator("input[name=name]").fill("astra-demo")
    f.locator("input[value=astra]").check()
    f.locator("input[name=scb]").set_input_files(f"{S}/scbzip/secure-connect-demo.zip")
    f.locator("input[name=astra_token]").fill("AstraCS:fake")
    f.locator("input[name=keyspace]").fill("ks1")
    pg.click("#conn-save")
    expect(pg.locator("#conn-list")).to_contain_text("secure-connect-demo.zip"); print("astra scb ok")
    # logout
    pg.click("#logout"); expect(pg.locator("#login-view")).to_be_visible(); print("logout ok")
    b.close()
print("PROBLEMS:", problems or "none")
