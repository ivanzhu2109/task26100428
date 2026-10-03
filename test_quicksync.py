"""QuickSync 自动化测试。"""

import asyncio
import random
import unittest
from collections import Counter

from quicksync import QuickSync


class TestDepthCorrection(unittest.IsolatedAsyncioTestCase):
    """长路径先把页面带到深度上限并完成读取，短路径随后到达：

    被截断的后代必须补齐，深度修正正确，且没有重复读取。
    """

    async def test_shorter_path_arrives_after_fetch_at_limit(self):
        # A(0) -> p1(1) -> p2(2) -> X(3)   长路径，X 初始在深度上限
        # A(0) -> s1(1) -> X(2)            短路径，随后才把 X 修正到 2
        # X -> Y -> Z                      Y 只有修正后(深度3)才可达，Z 始终不可达
        graph = {
            "A": ["p1", "s1"],
            "p1": ["p2"],
            "p2": ["X"],
            "s1": ["X"],
            "X": ["Y"],
            "Y": ["Z"],
            "Z": [],
        }
        fetched = []
        x_fetched = asyncio.Event()
        s1_gate = asyncio.Event()  # 拦住短路径，让长路径先跑完

        async def fetch(url):
            if url == "s1":
                await s1_gate.wait()
            fetched.append(url)
            if url == "X":
                x_fetched.set()
            return (f"payload:{url}", graph[url])

        qs = QuickSync(fetch)
        qs.submit("S", "A", 3)

        # 等长路径把 X 带到深度上限并完成读取
        await asyncio.wait_for(x_fetched.wait(), timeout=5)
        for _ in range(10):  # 让引擎消化 X 的完成事件
            await asyncio.sleep(0)
        self.assertNotIn("Y", fetched)  # 此时 Y 被深度上限截断

        # 放行短路径：X 的深度应被修正为 2，Y 因此进入可达范围
        s1_gate.set()
        result = await asyncio.wait_for(qs.finish(), timeout=5)

        self.assertEqual(result["S"], {"A", "p1", "p2", "s1", "X", "Y"})
        # Z 深度为 4，超出预算，不得被读取
        self.assertNotIn("Z", fetched)
        # 每个 URL 恰好读取一次
        self.assertEqual(len(fetched), len(set(fetched)))
        self.assertEqual(set(fetched), result["S"])


class TestSubmissionTiming(unittest.IsolatedAsyncioTestCase):
    """读取进行中提交、空闲后提交复用缓存；环、重复出链、深度 0；预算不串用。"""

    async def test_submit_during_fetch_and_after_idle(self):
        # A -> B(重复出链) ; B -> A(环), C ; C -> C(自环)
        graph = {
            "A": ["B", "B"],
            "B": ["A", "C"],
            "C": ["C"],
        }
        calls = []
        a_started = asyncio.Event()
        a_gate = asyncio.Event()

        async def fetch(url):
            if url == "A":
                a_started.set()
                await a_gate.wait()  # 拦住 A 的读取，制造“读取进行中”
            calls.append(url)
            return (f"payload:{url}", graph[url])

        qs = QuickSync(fetch)
        qs.submit("S1", "A", 2)

        # A 正在读取时，提交共享该页的新种子
        await asyncio.wait_for(a_started.wait(), timeout=5)
        qs.submit("S2", "A", 1)
        a_gate.set()

        # 等实例暂时空闲（A/B/C 都已读完且引擎消化完毕）
        for _ in range(1000):
            if len(calls) == 3:
                break
            await asyncio.sleep(0)
        self.assertEqual(sorted(calls), ["A", "B", "C"])
        for _ in range(10):
            await asyncio.sleep(0)

        # 空闲后继续提交：全部命中缓存，不应触发任何新读取
        qs.submit("S3", "B", 0)   # 深度 0 任务
        qs.submit("S4", "C", 1)   # 自环页面上的传播
        qs.submit("S5", "A", 1)   # 复用缓存继续传播
        results = await asyncio.wait_for(qs.finish(), timeout=5)

        self.assertEqual(results["S1"], {"A", "B", "C"})
        # S2 预算只有 1：C 虽被 S1 读到并缓存，也不得串用预算
        self.assertEqual(results["S2"], {"A", "B"})
        self.assertEqual(results["S3"], {"B"})
        self.assertEqual(results["S4"], {"C"})
        self.assertEqual(results["S5"], {"A", "B"})

        # 每个 URL 全程恰好读取一次，空闲后的提交没有产生新读取
        self.assertEqual(Counter(calls), Counter({"A": 1, "B": 1, "C": 1}))


class TestRandomizedAgainstReference(unittest.IsolatedAsyncioTestCase):
    """随机图 + 随机种子/深度 + 随机读取延迟，对照参考 BFS 逐种子核对。"""

    async def test_matches_reference_bfs(self):
        rng = random.Random(20261004)
        for trial in range(30):
            n = rng.randint(2, 12)
            nodes = [f"u{i}" for i in range(n)]
            graph = {
                u: [rng.choice(nodes) for _ in range(rng.randint(0, 4))]
                for u in nodes
            }
            seeds = {
                f"seed{k}": (rng.choice(nodes), rng.randint(0, 5))
                for k in range(rng.randint(1, 4))
            }

            calls = []

            async def fetch(url):
                if rng.random() < 0.5:
                    await asyncio.sleep(0)  # 随机打乱读取完成顺序
                calls.append(url)
                return (None, graph[url])

            qs = QuickSync(fetch)
            for sid, (url, depth) in seeds.items():
                qs.submit(sid, url, depth)
                if rng.random() < 0.5:
                    await asyncio.sleep(0)  # 随机交错提交时机
            results = await asyncio.wait_for(qs.finish(), timeout=5)

            # 参考实现：每个种子独立 BFS
            reachable_union = set()
            for sid, (url, max_depth) in seeds.items():
                expected = {url}
                frontier = {url}
                for _ in range(max_depth):
                    frontier = {v for u in frontier for v in graph[u]}
                    expected |= frontier
                reachable_union |= expected
                self.assertEqual(
                    results[sid], expected, f"trial={trial} seed={sid}"
                )

            # 每个 URL 最多读取一次，且不得读取任何种子预算都到不了的页面
            self.assertEqual(len(calls), len(set(calls)), f"trial={trial}")
            self.assertLessEqual(set(calls), reachable_union, f"trial={trial}")


if __name__ == "__main__":
    unittest.main()
