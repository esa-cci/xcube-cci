# The MIT License (MIT)
# Copyright (c) 2026 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio
import random
import threading
from typing import Optional

import aiohttp

from .constants import (DEFAULT_NUM_RETRIES, DEFAULT_RETRY_BACKOFF_BASE,
                        DEFAULT_RETRY_BACKOFF_MAX, LOG)


async def _run_with_session_executor(async_function, *params, headers):
    timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_connect=30, sock_read=120)
    connector = aiohttp.TCPConnector(limit=50, force_close=True)
    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
        trust_env=True,
        timeout=timeout,
    ) as session:
        return await async_function(session, *params)


class SessionExecutor:

    def __init__(
            self,
            user_agent: str = None,
            enable_warnings: bool = False,
            num_retries: int = DEFAULT_NUM_RETRIES,
            retry_backoff_max: int = DEFAULT_RETRY_BACKOFF_MAX,
            retry_backoff_base: float = DEFAULT_RETRY_BACKOFF_BASE,
    ):
        self._headers = {'User-Agent': user_agent} if user_agent else None
        self._enable_warnings = enable_warnings
        self._num_retries = num_retries
        self._retry_backoff_max = retry_backoff_max
        self._retry_backoff_base = retry_backoff_base

    def run_with_session(self, async_function, *params):
        coro = _run_with_session_executor(
            async_function, *params, headers=self._headers
        )
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        result_container = {}
        exception_container = {}

        def coro_runner():
            try:
                result_container["result"] = asyncio.run(coro)
            except Exception as e:
                exception_container["exception"] = e

        thread = threading.Thread(target=coro_runner, daemon=True)
        thread.start()
        thread.join()

        if "exception" in exception_container:
            raise exception_container["exception"]

        return result_container.get("result")

    def get_response_content(self, url: str) -> Optional[bytes]:
        return self.run_with_session(self.get_response_content_from_session, url)

    async def get_response_content_from_session(
            self, session: aiohttp.ClientSession, url: str
    ) -> Optional[bytes]:
        num_retries = self._num_retries
        retry_backoff_max = self._retry_backoff_max
        retry_backoff_base = self._retry_backoff_base
        for i in range(num_retries):
            retry_min = 100
            try:
                async with session.get(url) as resp:
                    retry_min = int(resp.headers.get('Retry-After', '100'))
                    if resp.status == 200:
                        return await resp.read()
                    elif 500 <= resp.status < 600:
                        error_message = f"Error {resp.status}: Cannot access url."
                        if self._enable_warnings:
                            LOG.warning(error_message)
                        return None
                    elif resp.status == 429:
                        error_message = "Error 429: Too Many Requests."
                    else:
                        return None
            except (
                    aiohttp.ClientConnectionError,
                    aiohttp.ServerDisconnectedError,
                    aiohttp.ClientPayloadError,
                    asyncio.TimeoutError,
            ) as e:
                error_message = str(e)
            retry_backoff = random.random() * retry_backoff_max
            retry_total = retry_min + retry_backoff
            if self._enable_warnings:
                LOG.info(
                    f"{error_message} - attempt {i + 1}/{num_retries}, "
                    f"retrying in {retry_total / 1000:.2f} s"
                )
            await asyncio.sleep(retry_total / 1000)
            retry_backoff_max *= retry_backoff_base

        return None
