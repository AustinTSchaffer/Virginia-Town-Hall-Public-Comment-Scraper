import aiohttp
import asyncio
import dataclasses
from dataclasses import dataclass
from typing import Iterable
import bs4
import re
from urllib.parse import urljoin
import os
import json


@dataclass
class PublicCommentListItem:
    page: int
    url: str
    title: str
    commenter: str
    date: str


@dataclass
class PublicComment:
    url: str
    page: int
    title: str
    commenter: str
    comment_text: str
    comment_html: str
    date: str


script_dir = os.path.dirname(os.path.realpath(__file__))
OUTPUT_FILE_NAME = "scraped_public_comments.jsonl"
OUTPUT_FILE_DIR = os.path.join(script_dir, "output")
OUTPUT_FILE_PATH = os.path.join(OUTPUT_FILE_DIR, OUTPUT_FILE_NAME)

PUBLIC_COMMENT_URL_REGEX = re.compile(r"viewcomments.cfm\?commentid=(\d+)")
PUBLIC_COMMENT_LIST_URL = (
    "https://townhall.virginia.gov/L/Comments.cfm?GdocForumID=1953"
)
TOTAL_PUBLIC_COMMENTS = 71297


async def fetch_comment_links(
    comment_list_url: str,
    total_comments: int,
    comments_per_page: int = 1000,
    starting_page: int = 1,
) -> Iterable[list[PublicCommentListItem]]:
    comments_fetched = 0
    current_page = starting_page
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/110.0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Connection": "keep-alive",
    }

    async with aiohttp.ClientSession() as session:
        while comments_fetched < total_comments:
            payload = f"vPage={current_page}&vPerPage={comments_per_page}&sub1=go"

            print(
                f'Fetching public comments page {current_page} (URL: "{comment_list_url}") (POST body: "{payload}") (Comments fetched so far: {comments_fetched})'
            )
            public_comments_on_current_page = []

            async with session.post(
                comment_list_url, data=payload, headers=headers
            ) as response:
                status = response.status
                if status < 200 or status >= 300:
                    raise ValueError(
                        f'HTTP Response Code from "{comment_list_url}": {status}'
                    )

                print(
                    f'Successfully fetched public comments page {current_page} (URL: "{comment_list_url}") (POST body: "{payload}") (Comments fetched so far: {comments_fetched})'
                )

                html = await response.text()
                soup = bs4.BeautifulSoup(html, "lxml")
                for content_section in soup.find_all(id="contentwide"):
                    for table_row in content_section.find_all("tr"):
                        links_in_row = table_row.find_all("a")

                        public_comment_links = [
                            link
                            for link in links_in_row
                            if "href" in link.attrs
                            and PUBLIC_COMMENT_URL_REGEX.match(link.attrs["href"])
                        ]

                        if not any(public_comment_links):
                            continue

                        comment_title, commenter, date = [
                            str.strip(td.text) for td in table_row.find_all("td")
                        ]

                        url = urljoin(
                            comment_list_url, public_comment_links[0].attrs["href"]
                        )

                        public_comments_on_current_page.append(
                            PublicCommentListItem(
                                url=url,
                                page=current_page,
                                title=comment_title,
                                commenter=commenter,
                                date=date,
                            )
                        )

            yield public_comments_on_current_page
            current_page += 1
            comments_fetched += len(public_comments_on_current_page)

async def fetch_public_comment(comment_stub: PublicCommentListItem) -> PublicComment | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/110.0",
    }

    print(f'Fetching public comment: "{comment_stub.url}"')

    async with aiohttp.ClientSession() as session:
        async with session.get(comment_stub.url, headers=headers) as response:
            status = response.status
            if status < 200 or status >= 300:
                print(f'WARNING: non-200 HTTP Response Code from "{comment_stub.url}": {status}')
                return None

            print(f'Successfully fetched public comment: "{comment_stub.url}"')

            html = await response.text()
            soup = bs4.BeautifulSoup(html, "lxml")
            content_section = soup.find(id="contentwide")
            comments = content_section.find_all(class_="divComment")
            if len(comments) > 1:
                print(
                    f'WARNING: This public comment has more than one comment: "{comment_stub.url}"'
                )

            comment = comments[0]
            return PublicComment(
                url=comment_stub.url,
                page=comment_stub.page,
                commenter=comment_stub.commenter,
                date=comment_stub.date,
                title=comment_stub.title,
                comment_text=comment.text,
                comment_html=comment.decode_contents(),
            )

async def main():
    public_comments_iterable = fetch_comment_links(
        PUBLIC_COMMENT_LIST_URL, TOTAL_PUBLIC_COMMENTS
    )

    async for page in public_comments_iterable:
        comments_per_chunk = 50
        page_chunks = [
            page[i : i + comments_per_chunk]
            for i in range(0, len(page), comments_per_chunk)
        ]

        for chunk in page_chunks:
            result = await asyncio.gather(
                *(fetch_public_comment(comment) for comment in chunk)
            )
            with open(OUTPUT_FILE_PATH, "a") as outfilehandle:
                for comment in result:
                    json.dump(dataclasses.asdict(comment), outfilehandle)
                    outfilehandle.write("\n")

    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
