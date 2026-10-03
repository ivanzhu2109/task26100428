"""QuickSync: 共享抓取的页面提取组件。

多个提取任务（种子）共享页面读取，各自独立计算可达范围。

- 同一实例内每个 URL 最多调用一次 fetch（含在途读取）。
- 每个种子独立维护“到达某页的最小深度”，结果互不影响。
- 页面先沿长路径被读到、随后出现更短路径时，会修正深度并继续传播，
  已缓存或在途的页面都参与修正。
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, Hashable, List, Optional, Set, Tuple

# 调用者提供的异步读取函数：fetch(url) -> (payload, [outlink_url, ...])
FetchFunc = Callable[[str], Awaitable[Tuple[Any, List[str]]]]


@dataclass
class _Seed:
    """单个种子任务的状态：深度预算与已知的每页最小深度。"""

    max_depth: int
    depths: Dict[str, int] = field(default_factory=dict)


@dataclass
class _Page:
    """全局页面缓存项；outlinks 为 None 表示读取尚未完成。"""

    outlinks: Optional[List[str]] = None


class QuickSync:
    """共享抓取调度器。

    - submit(seed_id, url, max_depth): 加入一个独立种子任务；可在读取进行
      中或实例暂时空闲时调用。
    - finish(): 关闭提交并等待全部任务收敛，返回 {seed_id: {url, ...}}。
    """

    def __init__(self, fetch: FetchFunc) -> None:
        self._fetch = fetch
        self._seeds: Dict[Hashable, _Seed] = {}
        self._pages: Dict[str, _Page] = {}
        # 待传播队列：(seed_id, url)，表示该种子对该页的深度需要向外扩散。
        self._relax: Deque[Tuple[Hashable, str]] = deque()
        # 已登记但尚未启动的读取。
        self._to_fetch: Deque[str] = deque()
        # 已完成读取、等待引擎处理的 URL。
        self._done: Deque[str] = deque()
        # 已启动（或已登记）且尚未被引擎消化的读取数。
        self._inflight = 0
        self._finishing = False
        self._wakeup: Optional[asyncio.Event] = None
        self._engine: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ API

    def submit(self, seed_id: Hashable, url: str, max_depth: int) -> None:
        if self._finishing:
            raise RuntimeError("finish 已开始，不能再提交")
        if seed_id in self._seeds:
            raise ValueError(f"种子标识重复: {seed_id!r}")
        if max_depth < 0:
            raise ValueError("max_depth 不能为负")
        self._seeds[seed_id] = _Seed(max_depth=max_depth, depths={url: 0})
        self._relax.append((seed_id, url))
        self._request_fetch(url)
        self._ensure_engine()
        self._wake()

    async def finish(self) -> Dict[Hashable, Set[str]]:
        self._finishing = True
        self._ensure_engine()
        self._wake()
        assert self._engine is not None
        await self._engine
        return {sid: set(seed.depths) for sid, seed in self._seeds.items()}

    # -------------------------------------------------------------- internals

    def _ensure_engine(self) -> None:
        if self._engine is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # 没有运行中的事件循环，finish() 时会再试
        self._wakeup = asyncio.Event()
        self._engine = loop.create_task(self._run())

    def _wake(self) -> None:
        if self._wakeup is not None:
            self._wakeup.set()

    def _request_fetch(self, url: str) -> None:
        """登记一次读取；每个 URL 全程只登记一次。"""
        if url in self._pages:
            return
        self._pages[url] = _Page()
        self._to_fetch.append(url)
        self._inflight += 1

    async def _fetch_one(self, url: str) -> None:
        _payload, outlinks = await self._fetch(url)
        # 出链去重（保持顺序），重复出链不重复传播。
        self._pages[url].outlinks = list(dict.fromkeys(outlinks))
        self._done.append(url)
        self._wake()

    def _propagate(self, seed_id: Hashable, url: str) -> None:
        seed = self._seeds[seed_id]
        depth = seed.depths[url]
        page = self._pages.get(url)
        if page is None or page.outlinks is None:
            return  # 读取未完成，完成时会重新入队传播
        if depth >= seed.max_depth:
            return  # 预算用尽，不再扩散
        for nxt in page.outlinks:
            nd = depth + 1
            if nd > seed.max_depth:
                continue
            known = seed.depths.get(nxt)
            if known is None or known > nd:
                seed.depths[nxt] = nd
                self._request_fetch(nxt)
                self._relax.append((seed_id, nxt))

    async def _run(self) -> None:
        assert self._wakeup is not None
        while True:
            while self._to_fetch:
                url = self._to_fetch.popleft()
                asyncio.create_task(self._fetch_one(url))
            while self._done:
                url = self._done.popleft()
                self._inflight -= 1
                for sid, seed in self._seeds.items():
                    if url in seed.depths:
                        self._relax.append((sid, url))
            while self._relax:
                sid, url = self._relax.popleft()
                self._propagate(sid, url)
            if self._to_fetch or self._done or self._relax:
                continue  # 传播又产生了新工作，先处理完再休息
            if self._finishing and self._inflight == 0:
                return
            await self._wakeup.wait()
            self._wakeup.clear()
