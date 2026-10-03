# task26100428

## QuickSync 共享抓取组件

纯 Python 标准库实现（`asyncio`），无第三方依赖。多个提取任务（种子）共享
页面读取，各自独立计算可达范围。

### 接口

```python
from quicksync import QuickSync

async def fetch(url):
    """调用者提供：返回 (payload, 出链 URL 列表)。假定总会成功。"""
    ...

qs = QuickSync(fetch)
qs.submit("seed-1", "https://example.com/a", max_depth=2)  # 可随时提交
qs.submit("seed-2", "https://example.com/b", max_depth=0)  # 读取进行中/空闲时均可
results = await qs.finish()  # 关闭提交并等待收敛
# results == {"seed-1": {...可达页面...}, "seed-2": {...}}
```

- `submit(seed_id, url, max_depth)`：登记独立种子任务，`seed_id` 互不重复；
  种子自身深度为 0，结果包含沿出链不超过 `max_depth` 条边可达的页面。
- `finish()`：异步关闭提交，等待全部读取与深度传播收敛后返回
  `{seed_id: 页面集合}`；调用后不得再 `submit`。

### 语义保证

- 同一实例内每个 URL 最多调用一次 `fetch`（在途读取亦去重），不同种子的
  结果可包含同一页面。
- 每个种子独立维护到达各页面的最佳剩余预算：短路径后到会修正页面深度，
  并用新预算继续补齐此前被截断的后代；缓存与在途页面均正确参与。
- 一个种子的预算不会串用到另一个种子；不读取任何已提交种子预算之外的页面。
- 暂时空闲不代表结束，后续提交的种子可复用已有缓存继续传播。

### 运行测试

```sh
python3 -m unittest test_quicksync -v
```
