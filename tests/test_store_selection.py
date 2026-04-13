import argparse
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import choose_store_playwright


class FakeLocator:
    def __init__(self, selector: str, page: "FakePage") -> None:
        self.selector = selector
        self.page = page

    @property
    def first(self) -> "FakeLocator":
        return self

    async def click(self, timeout: int | None = None) -> None:
        self.page.calls.append(("click", self.selector, timeout))

    async def fill(self, text: str, timeout: int | None = None) -> None:
        self.page.calls.append(("fill", self.selector, text, timeout))

    async def wait_for(self, timeout: int | None = None) -> None:
        self.page.calls.append(("wait_for", self.selector, timeout))

    def nth(self, index: int) -> "FakeLocator":
        self.page.calls.append(("nth", self.selector, index))
        return self


class FakePage:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def goto(self, url: str, wait_until: str | None = None, timeout: int | None = None) -> None:
        self.calls.append(("goto", url, wait_until, timeout))

    async def wait_for_load_state(self, state: str = "load", timeout: int | None = None) -> None:
        self.calls.append(("wait_for_load_state", state, timeout))

    async def wait_for_timeout(self, milliseconds: int) -> None:
        self.calls.append(("wait_for_timeout", milliseconds))

    def locator(self, selector: str) -> FakeLocator:
        self.calls.append(("locator", selector))
        return FakeLocator(selector, self)

    async def wait_for_selector(self, selector: str, timeout: int | None = None) -> None:
        self.calls.append(("wait_for_selector", selector, timeout))

    async def content(self) -> str:
        self.calls.append(("content",))
        return "<html></html>"


class StoreSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_choose_store_playwright_uses_woolworths_bookatimeslot_flow(self) -> None:
        page = FakePage()
        args = argparse.Namespace()
        args.manual_wait_seconds = 0

        selected_store = await choose_store_playwright(
            page,
            "https://www.woolworths.co.nz/shop/browse/beer-wine?page=1&size=48",
            args,
            "Karori Woolworths",
            0,
        )

        self.assertEqual(selected_store, "Karori Woolworths")
        self.assertIn(("goto", "https://www.woolworths.co.nz/bookatimeslot", "domcontentloaded", 60000), page.calls)
        self.assertIn(("locator", r"text=/Change\s+Address/i"), page.calls)
        self.assertIn(("wait_for_selector", r"text=/Choose a delivery address/i", 15000), page.calls)
        self.assertIn(("fill", "input[type='search'], input[placeholder*='Search'], input[placeholder*='Address'], input[placeholder*='Suburb'], input[aria-label*='search']", "Karori Woolworths", 5000), page.calls)
        self.assertIn(("locator", r"text=/Save and Continue Shopping/i"), page.calls)

    async def test_choose_store_playwright_falls_back_to_store_finder_when_no_store_name(self) -> None:
        page = FakePage()
        args = argparse.Namespace()
        args.manual_wait_seconds = 0

        selected_store = await choose_store_playwright(
            page,
            "https://www.woolworths.co.nz/shop/browse/beer-wine?page=1&size=48",
            args,
            None,
            0,
        )

        self.assertEqual(selected_store, "Woolworths")
        self.assertIn(("goto", "https://www.woolworths.co.nz/bookatimeslot", "domcontentloaded", 60000), page.calls)
        self.assertIn(("goto", "https://www.woolworths.co.nz/store-finder", "domcontentloaded", 60000), page.calls)


if __name__ == "__main__":
    unittest.main()
