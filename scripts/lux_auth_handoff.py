from __future__ import annotations

import argparse
import asyncio
import os
import re
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ove_scraper import auth_lockout
from ove_scraper.automation_lock import OveAutomationLock, lock_name_for_port


DEFAULT_BASE_URL = "https://api.agiopen.org"
DEFAULT_MODEL = "lux-actor-1"
ENV_FILES = (".env.staging", ".env", r"C:\Users\joema\Auction Module\.env")
SYNC_BROWSER_SCRIPT = Path(r"C:\Users\joema\Auction Module\scripts\start_ove_browser_sync.ps1")
HOTDEAL_BROWSER_SCRIPT = Path(r"C:\Users\joema\Auction Module\scripts\start_ove_browser.ps1")


class BytesImage:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


@dataclass
class BrowserBridge:
    page: Any
    viewport_width: int = 1280
    viewport_height: int = 720
    last_image: BytesImage | None = None

    async def refresh_viewport(self) -> tuple[int, int]:
        try:
            dims = await self.page.evaluate("""() => ({w: window.innerWidth, h: window.innerHeight})""")
            self.viewport_width = int(dims["w"] or self.viewport_width)
            self.viewport_height = int(dims["h"] or self.viewport_height)
        except Exception:
            pass
        return self.viewport_width, self.viewport_height

    def scale(self, x: int, y: int) -> tuple[int, int]:
        return (
            max(0, min(round(x * self.viewport_width / 1000), max(self.viewport_width - 1, 0))),
            max(0, min(round(y * self.viewport_height / 1000), max(self.viewport_height - 1, 0))),
        )


class CdpImageProvider:
    def __init__(self, bridge: BrowserBridge):
        self.bridge = bridge

    async def __call__(self) -> BytesImage:
        await self.bridge.refresh_viewport()
        data = await self.bridge.page.screenshot(
            type="png",
            timeout=120000,
            animations="disabled",
            caret="hide",
            scale="css",
        )
        image = BytesImage(data)
        self.bridge.last_image = image
        return image

    async def last_image(self) -> BytesImage:
        if self.bridge.last_image is None:
            return await self()
        return self.bridge.last_image


class CdpActionHandler:
    def __init__(self, bridge: BrowserBridge):
        self.bridge = bridge

    async def __call__(self, actions):
        for action in actions:
            count = action.count or 1
            for _ in range(count):
                await self._run_one(action)
        await self.bridge.page.wait_for_timeout(250)

    async def _run_one(self, action):
        arg = action.argument or ""
        action_type = action.type.value if hasattr(action.type, "value") else str(action.type)
        if action_type == "click":
            x, y = self._parse_xy(arg)
            await self.bridge.page.mouse.click(x, y)
        elif action_type == "left_double":
            x, y = self._parse_xy(arg)
            await self.bridge.page.mouse.click(x, y, click_count=2)
        elif action_type == "hotkey":
            keys = [self._map_key(part.strip()) for part in re.split(r"[,+]", arg.strip("()")) if part.strip()]
            if keys:
                await self.bridge.page.keyboard.press("+".join(keys))
        elif action_type == "type":
            await self.bridge.page.keyboard.insert_text(arg)
        elif action_type == "scroll":
            x, y, direction = self._parse_scroll(arg)
            await self.bridge.page.mouse.move(x, y)
            await self.bridge.page.mouse.wheel(0, -700 if direction == "down" else 700)
        elif action_type == "wait":
            await self.bridge.page.wait_for_timeout(1000)
        elif action_type in {"finish", "fail", "call_user"}:
            return

    def _parse_xy(self, arg: str) -> tuple[int, int]:
        x_raw, y_raw, *_ = [p.strip() for p in arg.split(",")]
        return self.bridge.scale(int(x_raw), int(y_raw))

    def _parse_scroll(self, arg: str) -> tuple[int, int, str]:
        x_raw, y_raw, direction, *_ = [p.strip() for p in arg.split(",")]
        x, y = self.bridge.scale(int(x_raw), int(y_raw))
        return x, y, direction.lower()

    def _map_key(self, key: str) -> str:
        return {
            "ctrl": "Control",
            "control": "Control",
            "shift": "Shift",
            "alt": "Alt",
            "enter": "Enter",
            "return": "Enter",
            "tab": "Tab",
            "esc": "Escape",
            "escape": "Escape",
            "space": "Space",
        }.get(key.lower(), key)


def load_env_files() -> None:
    root = Path(__file__).resolve().parent.parent
    for name in ENV_FILES:
        path = Path(name)
        if not path.is_absolute():
            path = root / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def is_port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


async def ensure_browser(port: int, track: str) -> None:
    if is_port_open(port):
        return
    script = SYNC_BROWSER_SCRIPT if track == "sync" else HOTDEAL_BROWSER_SCRIPT
    subprocess.Popen([
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script),
    ])
    for _ in range(60):
        if is_port_open(port):
            return
        await asyncio.sleep(1)
    raise RuntimeError(f"Chrome CDP port {port} never opened")


def pick_page(context):
    pages = list(context.pages)
    if not pages:
        return None

    def score(page):
        url = (page.url or "").lower()
        if "auth.manheim.com" in url:
            return 100
        if "ove.com" in url and "/saved_searches" in url:
            return 90
        if "ove.com" in url and "/search/results" in url:
            return 80
        if "ove.com" in url:
            return 70
        if url == "about:blank":
            return 10
        return 0

    return sorted(pages, key=score, reverse=True)[0]


async def compact_context_tabs(context, keep_page=None) -> int:
    pages = [page for page in list(context.pages) if not page.is_closed()]
    if len(pages) <= 1:
        return 0

    auth_patterns = (
        "auth.manheim.com",
        "auth0.manheim.com",
        "signin.manheim.com",
        "/as/authorization",
        "/as/login",
        "accounts.manheim.com",
    )

    def url_for(page) -> str:
        try:
            if page.is_closed():
                return ""
            return (page.url or "").lower()
        except Exception:
            return ""

    def is_scraper_owned(url: str) -> bool:
        return (
            url == "about:blank"
            or "ove.com" in url
            or any(pattern in url for pattern in auth_patterns)
        )

    def score(page) -> int:
        if keep_page is not None and page is keep_page:
            return 1000
        url = url_for(page)
        if not url:
            return -100
        if "auth.manheim.com" in url or "signin.manheim.com" in url:
            return 200
        if "ove.com" in url:
            value = 100
            if "/saved_searches" in url:
                value += 30
            if "/search/results" in url:
                value += 20
            if "/details/" in url:
                value += 10
            return value
        if url == "about:blank":
            return 0
        return -10

    scraper_pages = [page for page in pages if is_scraper_owned(url_for(page))]
    if len(scraper_pages) <= 1:
        return 0
    keep = max(scraper_pages, key=score)
    closed = 0
    for page in scraper_pages:
        if page is keep:
            continue
        try:
            url = page.url
        except Exception:
            url = "(unknown)"
        print(f"closing extra scraper tab: {url}")
        try:
            await page.close()
            closed += 1
        except Exception:
            pass
    return closed


async def page_state(page) -> dict[str, Any]:
    try:
        return await page.evaluate(
            """
            () => {
              const text = `${document.title || ""} ${document.body?.innerText || ""}`.toLowerCase();
              return {
                url: location.href,
                title: document.title || "",
                hasPassword: !!document.querySelector("input[type='password']"),
                passwordFilled: !!document.querySelector("input[type='password']")?.value,
                savedSearchCards: document.querySelectorAll("[data-test-id^='search name:'], .SavedSearchItem__container").length,
                vehicleCards: document.querySelectorAll("[data-test-id*='vehicle'], [class*='VehicleCard'], tr[data-test-id*='vehicle']").length,
                hasLoginCopy: /sign\\s*(in|on)|password|verification|authentication|login/.test(text),
                hasTwoFactorCopy: /verification code|security code|two-factor|multi-factor|authenticator|text message/.test(text),
                hasNoSavedSearches: /no saved searches/i.test(document.body?.innerText || ""),
              };
            }
            """
        )
    except Exception:
        return {"url": page.url, "title": ""}


def is_authenticated(state: dict[str, Any], track: str) -> bool:
    url = str(state.get("url") or "").lower()
    if "auth.manheim.com" in url or state.get("hasPassword") or state.get("hasTwoFactorCopy"):
        return False
    if track == "sync":
        return int(state.get("savedSearchCards") or 0) > 0 or int(state.get("vehicleCards") or 0) > 0
    return "ove.com" in url and not state.get("hasLoginCopy")


async def run_handoff(port: int, track: str, model: str, artifact_dir: Path) -> int:
    try:
        from oagi import TaskerAgent
        from playwright.async_api import async_playwright
    except Exception as exc:
        print(f"Missing dependency for Lux auth handoff: {exc}", file=sys.stderr)
        return 2

    await ensure_browser(port, track)
    with OveAutomationLock(name=lock_name_for_port(port), timeout_seconds=900):
        lockout = auth_lockout.get_state(artifact_dir, port=port)
        if lockout.blocked:
            print(f"Lux auth refused by auth lockout on port {port}: {lockout.reason}", file=sys.stderr)
            return 3

        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
            page = pick_page(context) or await context.new_page()
            await page.bring_to_front()
            target = "https://www.ove.com/saved_searches" if track == "sync" else "https://www.ove.com/"
            try:
                await page.goto(target, wait_until="domcontentloaded", timeout=60000)
            except Exception as exc:
                print(f"Initial OVE navigation warning: {exc}")
            await page.wait_for_timeout(3000)
            closed_before = await compact_context_tabs(context, page)
            if closed_before:
                print(f"compact-tabs pre-auth closed {closed_before} extra tab(s)")

            before = await page_state(page)
            print(f"pre-auth state: {before}")
            if is_authenticated(before, track):
                print("Already authenticated; Playwright can take over.")
                closed_after = await compact_context_tabs(context, page)
                if closed_after:
                    print(f"compact-tabs post-auth closed {closed_after} extra tab(s)")
                return 0
            if before.get("hasTwoFactorCopy"):
                print("Two-factor prompt is visible; operator action is required.", file=sys.stderr)
                return 4

            post_record = auth_lockout.record_login_click(artifact_dir, port=port)
            if post_record.blocked:
                print(f"Lux auth refused after ledger update: {post_record.reason}", file=sys.stderr)
                return 3

            task = (
                "You are controlling the current OVE/Manheim browser page for an automated scraper. "
                "Complete only the login/authentication step using visible page controls. "
                "If Chrome has saved credentials filled, click Sign On or Sign In. "
                "If asked to trust this device, choose the option that keeps this browser trusted. "
                "Do not navigate with the address bar, do not change browser settings, and do not attempt "
                "the export or vehicle workflow. Stop as soon as OVE is authenticated and the page shows "
                "saved searches or normal OVE content. If a verification code or missing password blocks "
                "you, stop and report failure."
            )
            todos = [
                "Use only page-content controls.",
                "Submit the saved Manheim credentials if they are already present.",
                "Preserve or accept device trust if prompted.",
                "Stop once OVE content is visible.",
            ]
            bridge = BrowserBridge(page)
            agent = TaskerAgent(model=model)
            agent.set_task(task=task, todos=todos)
            result = await agent.execute(
                instruction=task,
                action_handler=CdpActionHandler(bridge),
                image_provider=CdpImageProvider(bridge),
            )
            await page.wait_for_timeout(5000)
            after = await page_state(page)
            print(f"Lux result: {result}")
            print(f"post-auth state: {after}")
            closed_after = await compact_context_tabs(context, page)
            if closed_after:
                print(f"compact-tabs post-auth closed {closed_after} extra tab(s)")
            if is_authenticated(after, track):
                auth_lockout.record_success(artifact_dir, port=port)
                return 0
            if after.get("hasTwoFactorCopy"):
                print("Two-factor prompt remains visible; operator action is required.", file=sys.stderr)
                return 4
            return 1


async def main() -> int:
    load_env_files()
    parser = argparse.ArgumentParser(description="Lux auth handoff for OVE Chrome profiles")
    parser.add_argument("--track", choices=("sync", "hot-deal"), required=True)
    parser.add_argument("--port", type=int)
    parser.add_argument("--model", default=os.getenv("OAGI_MODEL", DEFAULT_MODEL))
    parser.add_argument("--base-url", default=os.getenv("OAGI_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--artifact-dir", default=os.getenv("ARTIFACT_DIR", "./artifacts"))
    args = parser.parse_args()

    if not os.getenv("OAGI_API_KEY"):
        print("Missing OAGI_API_KEY in .env.staging or environment.", file=sys.stderr)
        return 2
    os.environ["OAGI_BASE_URL"] = args.base_url
    port = args.port or (9223 if args.track == "sync" else 9222)
    return await run_handoff(port, args.track, args.model, Path(args.artifact_dir))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
