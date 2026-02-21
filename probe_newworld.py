import asyncio
import re
from playwright.async_api import async_playwright

URL = "https://www.newworld.co.nz/shop/category/beer-wine-and-cider/beer/new-zealand-beers?pg=1"


async def main() -> None:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        response = await page.goto(URL, wait_until="networkidle")
        await page.wait_for_timeout(3000)

        print("status", response.status if response else None)
        print("url", page.url)
        print("title", await page.title())

        body_text = await page.locator("body").inner_text()
        print("body_preview", body_text[:800].replace("\n", " | "))

        html = await page.content()
        print("html_len", len(html))

        candidates = [
            '[data-testid="product-tile"]',
            '[data-testid*="product"]',
            '.fs-product-card',
            '.product-card',
            '[class*="product"]',
            '[class*="tile"]',
            'article',
        ]
        for selector in candidates:
            count = await page.locator(selector).count()
            print("selector", selector, count)

        testids = re.findall(r'data-testid="([^"]+)"', html, flags=re.I)
        product_testids = sorted({value for value in testids if "product" in value.lower()})
        print("product_testids", product_testids[:40])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
