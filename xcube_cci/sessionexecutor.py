# The MIT License (MIT)
# Copyright (c) 2025 ESA Climate Change Initiative
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
import time
from typing import Optional

import aiohttp

from .constants import (DEFAULT_NUM_RETRIES, DEFAULT_RETRY_BACKOFF_BASE,
                        DEFAULT_RETRY_BACKOFF_MAX, LOG)


async def _run_with_session_executor(async_function, *params, headers):
    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=50),
            headers=headers,
            trust_env=True
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
        # See https://github.com/aio-libs/aiohttp/blob/master/docs/
        # client_advanced.rst#graceful-shutdown
        loop = asyncio.new_event_loop()
        coro = _run_with_session_executor(
            async_function, *params, headers=self._headers
        )
        result = loop.run_until_complete(coro)
        # Short sleep to allow underlying connections to close
        loop.run_until_complete(asyncio.sleep(1.))
        loop.close()
        return result

    def get_response_content(self, url: str) -> Optional[bytes]:
        return self.run_with_session(self.get_response_content_from_session, url)

    async def get_response_content_from_session(
            self, session: aiohttp.ClientSession,
            url: str
    ) -> Optional[bytes]:
        num_retries = self._num_retries
        retry_backoff_max = self._retry_backoff_max  # ms
        retry_backoff_base = self._retry_backoff_base
        for i in range(num_retries):
            resp = await session.request(method='GET', url=url)
            if resp.status == 200:
                try:
                    content = await resp.read()
                    return content
                except aiohttp.client_exceptions.ClientPayloadError as cpe:
                    error_message =str(cpe)
            elif 500 <= resp.status < 600:
                if self._enable_warnings:
                    error_message = f'Error {resp.status}: Cannot access url.'
                    LOG.warning(error_message)
                return None
            elif resp.status == 429:
                error_message = "Error 429: Too Many Requests."
            else:
                break
            # Retry after 'Retry-After' with exponential backoff
            retry_min = int(resp.headers.get('Retry-After', '100'))
            retry_backoff = random.random() * retry_backoff_max
            retry_total = retry_min + retry_backoff
            if self._enable_warnings:
                retry_message = \
                    f'{error_message} ' \
                    f'Attempt {i + 1} of {num_retries} to retry after ' \
                    f'{"%.2f" % retry_min} + {"%.2f" % retry_backoff} = ' \
                    f'{"%.2f" % retry_total} ms ...'
                LOG.info(retry_message)
            time.sleep(retry_total / 1000.0)
            retry_backoff_max *= retry_backoff_base
        return None
