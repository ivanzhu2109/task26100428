"""QuickSync 共享抓取组件。

多个提取任务（种子）共享页面读取，各自独立计算可达范围。
调用者提供异步函数 fetch(url)，返回 (payload, 出链 URL 列表)。

核心不变量：
- 同一实例内每个 URL 最多调用一次 fetch（含在途读取去重）。
- 每个种子独立记录它到达各 URL 的“最佳剩余深度预算”；只有以更优预算
  重新到达时，才继续向出链传播，因此短路径后到会修正深度并补齐后代。
- 只读取已被某个已提交种子的预算允许到达的页面。
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Dict, Hashable, List, Set, Tuple

# fetch(url) -> (payload, links)；payload 是不透明数据，本组件不解释。
FetchFunc = Callable[[str], Awaitable[Tuple[Any, List[str]]]]


class QuickSync:
    """共享抓取调度器。

    - submit(seed_id, url, max_depth)：登记一个独立种子任务。
      允许在其他读取进行中或实例暂时空闲时提交。
    - finish()：关闭提交，等待全部读取与深度传播收敛，
      返回 {seed_id: 该种子可达页面集合}。
    """

    def __init__(self, fetch: FetchFunc) -> None:
        self._fetch = fetch
        self._cache: Dict[str, Tuple[Any, List[str]]] = {}
        self._inflight: Dict[str, "asyncio.Task[None]"] = {}
        # 等待某 URL 读取完成的 (seed_id, 剩余预算) 认领，预算均 > 0。
        self._claims: Dict[str, List[Tuple[Hashable, int]]] = {}
        self._best: Dict[Hashable, Dict[str, int]] = {}
        self._results: Dict[Hashable, Set[str]] = {}
        self._queue: Deque[Tuple[Hashable, str, int]] = deque()
        # 未完成工作量 = 队列中待处理项 + 在途 fetch 数。
        self._active = 0
        self._closed = False
        self._changed = asyncio.Event()
        self._pump_task: "asyncio.Task[None] | None" = None

    # ---------------------------------------------------------------- 提交

    def submit(self, seed_id: Hashable, url: str, max_depth: int) -> None:
        if self._closed:
            raise RuntimeError("finish() 已开始，不能再提交")
        if seed_id in self._best:
            raise ValueError(f"种子标识重复: {seed_id!r}")
        if max_depth < 0:
            raise ValueError("max_depth 不能为负")
        self._best[seed_id] = {}
        self._results[seed_id] = set()
        self._enqueue(seed_id, url, max_depth)
        self._ensure_pump()

    # ---------------------------------------------------------------- 收尾

    async def finish(self) -> Dict[Hashable, Set[str]]:
        self._closed = True
        self._ensure_pump()
        self._changed.set()
        assert self._pump_task is not None
        await self._pump_task
        return {seed_id: set(urls) for seed_id, urls in self._results.items()}

    # ---------------------------------------------------------------- 内部

    def _ensure_pump(self) -> None:
        if self._pump_task is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return  # 尚无事件循环，finish() 时会再尝试。
            self._pump_task = loop.create_task(self._pump())

    def _enqueue(self, seed_id: Hashable, url: str, budget: int) -> None:
        best = self._best[seed_id]
        if budget <= best.get(url, -1):
            return
        best[url] = budget
        self._results[seed_id].add(url)
        self._queue.append((seed_id, url, budget))
        self._active += 1
        self._changed.set()

    def _process(self, seed_id: Hashable, url: str, budget: int) -> None:
        cached = self._cache.get(url)
        if cached is not None:
            if budget > 0:
                for link in cached[1]:
                    self._enqueue(seed_id, link, budget - 1)
            return
        if budget > 0:
            self._claims.setdefault(url, []).append((seed_id, budget))
        if url not in self._inflight:
            loop = asyncio.get_running_loop()
            self._inflight[url] = loop.create_task(self._run_fetch(url))
            self._active += 1

    async def _run_fetch(self, url: str) -> None:
        try:
            payload, links = await self._fetch(url)
        finally:
            self._inflight.pop(url, None)
            self._active -= 1
        self._cache[url] = (payload, links)
        claims = self._claims.pop(url, [])
        for seed_id, budget in claims:
            for link in links:
                self._enqueue(seed_id, link, budget - 1)
        self._changed.set()

    async def _pump(self) -> None:
        while True:
            while self._queue:
                seed_id, url, budget = self._queue.popleft()
                self._active -= 1
                self._process(seed_id, url, budget)
            if self._closed and self._active == 0:
                return
            self._changed.clear()
            await self._changed.wait()
