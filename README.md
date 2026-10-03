# QuickSync 共享抓取组件

纯 Python 标准库实现（`asyncio`），无任何第三方依赖。

多个提取任务（种子）共享页面读取：同一实例内每个 URL 最多 `fetch` 一次，
但每个种子独立计算自己在深度预算内的可达页面集合。

## 接口

```python
from quicksync import QuickSync

qs = QuickSync(fetch)            # fetch: async (url) -> (payload, [outlink_url, ...])
qs.submit(seed_id, url, max_depth)  # 加入一个种子任务；可在读取进行中或空闲时调用
results = await qs.finish()      # 关闭提交、等待收敛，返回 {seed_id: {url, ...}}
```

- `submit(seed_id, url, max_depth)`：`seed_id` 互不相同；种子自身深度为 0，
  结果恰好包含沿出链不超过 `max_depth` 条边可达的页面。
- `finish()`：开始后因仍有读取或深度传播未完成而不会提前返回；
  开始后调用者不再提交（再 `submit` 会抛 `RuntimeError`）。
- 页面先沿长路径被读到、随后出现更短路径时，会自动修正深度并继续传播；
  已缓存或在途的页面都参与修正，其他种子的缓存不会绕过当前种子的深度限制。
- 不会读取任何已提交种子的预算都到达不了的页面。

## 示例

```python
import asyncio
from quicksync import QuickSync

GRAPH = {"A": ["B", "C"], "B": ["C"], "C": []}

async def fetch(url):
    return (f"payload:{url}", GRAPH[url])

async def main():
    qs = QuickSync(fetch)
    qs.submit("deep", "A", 2)
    qs.submit("shallow", "A", 1)
    print(await qs.finish())
    # {'deep': {'A', 'B', 'C'}, 'shallow': {'A', 'B', 'C'}}

asyncio.run(main())
```

## 运行测试

```sh
python3 -m unittest -v
```
