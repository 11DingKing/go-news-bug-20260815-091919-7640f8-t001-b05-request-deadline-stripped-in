## 题目元信息

- 来源：`self_built`
- 处理流程：`Codex Gold 修复`
- Bug 分类：`context`
- 复现稳定性：`stable`
- 难度：`advanced`
- 目标 Bug 数量：`1`
- 上游 Issue：无（0-1 自建项目）

# BUG_TASK

- 来源（source）：`self_built`
- 题目（title）：慢速写入时请求 deadline 被忽略，操作越过客户端超时继续执行并持久化
- Bug 分类（category）：上下文（context）/ 取消失败（cancellation_failure）

## 用户可见症状

存储写入变慢时，客户端为请求设置的 deadline 到期后，服务端操作并不会随之中止，
而是继续执行直到写入完成。此时客户端早已超时放弃，服务端也无法再把响应送回，
等于为一次「已经无人等待」的操作白白占用连接与锁。

客户端在超时后通常会重试，于是同一事件被再次提交，形成重复工作与无谓的资源占用。
在压测或联调时，这一现象极易被误判为「存储太慢、需要扩容」——因为看起来就是写盘
耗时过长；但真正的异常是：请求的 deadline 没有贯穿整个调用链，本应在超时时刻返回
`context.DeadlineExceeded` 并释放资源的阻塞写，却继续跑到了终点。

## 正确行为

请求的 deadline 必须从 HTTP 处理入口一路贯穿到存储写入，全程不得中断或丢失。当写入
超过 deadline 时，操作应立即返回 `context.DeadlineExceeded`，中止写入并释放已占用的
连接与锁，不再继续执行；被中止的事件不得被持久化。重试提交的重复事件仍应被幂等去重。

## 稳定复现命令

```sh
go test -run TestDeadlinePropagatesThroughPipeline -count=1 ./internal/ingest/
```

该测试使用一个「延迟 200ms、但遵守 context 取消」的慢速存储，配合一个 50ms 的请求
deadline：先断言 Ingest 返回 `context.DeadlineExceeded`，再在足够长的时间后断言事件
没有被持久化。当前实现下，写入会越过 deadline 继续完成并落盘，导致断言稳定失败。

## 验收标准

1. 正常路径：未超时的事件正常落盘并返回成功。
2. 失败/并发路径：写入变慢且超过 deadline 时，操作返回 `context.DeadlineExceeded`，
   立即中止且不继续占用连接与锁，被中止的事件不得持久化。
3. 无回归断言：取消或超时后无 goroutine 泄漏；重试的重复事件仍被幂等去重，不产生
   重复落盘。
4. 仓库保持可构建：`go build ./...` 与 `go test -run '^$' -count=1 ./...` 通过，
   `go test -count=1 ./...` 中该聚焦测试稳定通过（不再失败）。
