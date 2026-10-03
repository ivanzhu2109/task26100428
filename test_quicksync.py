"""QuickSync 自动化测试（仅标准库，unittest + asyncio）。"""
import asyncio
import unittest

from quicksync import QuickSync


class TestBasic(unittest.IsolatedAsyncioTestCase):
    async def test_chain_depths_and_single_fetch(self):
        graph = {
            "A": ["B", "C"],
            "B": ["D"],
            "C": ["D"],
            "D": [],
        }
        calls = []

        async def fetch(url):
            calls.append(url)
            return f"payload:{url}", graph[url]

        qs = QuickSync(fetch)
        qs.submit("s1", "A", 1)
        qs.submit("s2", "A", 2)
        results = await qs.finish()

        self.assertEqual(results["s1"], {"A", "B", "C"})
        self.assertEqual(results["s2"], {"A", "B", "C", "D"})
        # 每个 URL 全局最多读取一次，两个种子共享缓存。
        self.assertEqual(sorted(calls), ["A", "B", "C", "D"])

    async def test_duplicate_seed_and_late_submit_rejected(self):
        async def fetch(url):
            return None, []

        qs = QuickSync(fetch)
        qs.submit("s", "A", 0)
        with self.assertRaises(ValueError):
            qs.submit("s", "B", 0)
        await qs.finish()
        with self.assertRaises(RuntimeError):
            qs.submit("t", "A", 0)


class TestDepthCorrection(unittest.IsolatedAsyncioTestCase):
    async def test_shorter_path_after_depth_limit_backfills_descendants(self):
        # 长路径: R -> L1 -> L2 -> X （X 恰好到深度上限 3，预算耗尽）
        # 短路径: R -> S1 -> X       （X 深度 2，剩余预算 1，应补出 D1）
        graph = {
            "R": ["L1", "S1"],
            "L1": ["L2"],
            "L2": ["X"],
            "S1": ["X"],
            "X": ["D1"],
            "D1": ["D2"],
            "D2": [],
        }
        x_fetched = asyncio.Event()
        calls = []

        async def fetch(url):
            if url == "S1":
                # 卡住短路径，保证 X 先沿长路径在深度上限处被读完。
                await x_fetched.wait()
            await asyncio.sleep(0)
            if url == "X":
                x_fetched.set()
            calls.append(url)
            return f"payload:{url}", graph[url]

        qs = QuickSync(fetch)
        qs.submit("s", "R", 3)
        results = await qs.finish()

        # X 最终深度为 2，被截断的后代 D1（深度 3）被补齐；
        # D2 深度 4 超出预算，不得出现。
        self.assertEqual(results["s"], {"R", "L1", "L2", "S1", "X", "D1"})
        # 没有重复读取，也没有读取不可达的 D2。
        self.assertEqual(sorted(calls), ["D1", "L1", "L2", "R", "S1", "X"])


class TestSharedAndIdleResubmit(unittest.IsolatedAsyncioTestCase):
    async def test_submit_during_fetch_and_after_idle(self):
        # 含环（A<->B、C 自环、D->B）与重复出链（A->B 两次）。
        graph = {
            "A": ["B", "B"],
            "B": ["A", "C"],
            "C": ["C", "D"],
            "D": ["B"],
        }
        b_started = asyncio.Event()
        release_b = asyncio.Event()
        d_fetched = asyncio.Event()
        calls = []

        async def fetch(url):
            if url == "B":
                b_started.set()
                await release_b.wait()
            await asyncio.sleep(0)
            calls.append(url)
            if url == "D":
                d_fetched.set()
            return f"payload:{url}", graph[url]

        qs = QuickSync(fetch)
        qs.submit("a", "A", 2)

        # 等 B 的在途读取开始，期间提交共享该页的新种子 b 与深度为 0 的 c。
        await b_started.wait()
        qs.submit("b", "B", 2)
        qs.submit("c", "C", 0)
        release_b.set()

        # 等实例暂时空闲（D 的读取完成、传播落定）后，再提交复用缓存的 d。
        await d_fetched.wait()
        for _ in range(5):
            await asyncio.sleep(0)
        calls_before_d = list(calls)
        qs.submit("d", "A", 3)

        results = await qs.finish()

        # 逐种子核对：预算互不串用。
        self.assertEqual(results["a"], {"A", "B", "C"})        # D 深度 3，超限
        self.assertEqual(results["b"], {"A", "B", "C", "D"})   # D 深度 2，恰好可达
        self.assertEqual(results["c"], {"C"})                  # 深度 0，只有自身
        self.assertEqual(results["d"], {"A", "B", "C", "D"})   # 全程命中缓存
        # 每个 URL 全局恰好读取一次；d 的加入没有触发任何新读取。
        self.assertEqual(sorted(calls), ["A", "B", "C", "D"])
        self.assertEqual(calls, calls_before_d)


if __name__ == "__main__":
    unittest.main()
