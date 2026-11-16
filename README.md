# 园区电表逐小时结算 API

本地可启动的 FastAPI + SQLite 服务：录入电表累计读数，按连续小时发布可复核的结算明细。

## 启动

### Docker Compose

```bash
docker compose up --build -d
curl -s http://127.0.0.1:8000/healthz
```

- API：<http://127.0.0.1:8000>
- 交互文档 `/docs`，OpenAPI `/openapi.json`
- SQLite 数据保存在命名卷 `energy-data` 的 `/data/energy.sqlite3`，容器重建不丢数据
- 换端口：`API_PORT=8080 docker compose up --build -d`

### 本地直接运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

数据库路径由 `ENERGY_DB_PATH` 指定，默认 `./data/energy.sqlite3`，启动时自动建表。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/meters` | 登记电表；重复登记幂等（200 vs 201） |
| GET | `/meters` | 电表列表与读数概况 |
| GET | `/meters/{meter_id}` | 单表概况（时刻数、版本数、最新时刻） |
| POST | `/meters/{meter_id}/readings` | 批量录入读数，整批原子 |
| GET | `/meters/{meter_id}/readings` | 查询读数；`include_history=true` 返回全部修订 |
| POST | `/meters/{meter_id}/settlements` | 发布连续 1～48 小时的结算 |
| GET | `/meters/{meter_id}/settlements` | 按表列结算摘要 |
| GET | `/settlements` | 全部结算摘要 |
| GET | `/settlements/{settlement_no}` | 结算明细（发布时快照） |
| GET | `/settlement-events` | 结算变更流（递增游标分页，供数仓增量接入） |
| GET | `/healthz` | 健康检查 |

交互式文档里可直接试调所有接口。

## 数据规则

### 读数

请求体：

```json
{
  "readings": [
    {"hour": "2026-11-16T00:00:00Z", "value": 9530, "revision": 1},
    {"hour": "2026-11-16T01:00:00Z", "value": 9610, "revision": 1}
  ]
}
```

- `hour` 必须是 UTC 整点字符串（`Z` 或 `+00:00`，分秒为 0），统一存储为 `YYYY-MM-DDTHH:00:00Z`。
- `value` 为 0～9999 的整数累计读数；`revision` 为正整数。
- 版本只追加不覆盖，唯一键是 `(meter_id, hour, revision)`。

同一批次的裁决规则：

| 情况 | 结果 |
| --- | --- |
| 新时刻 | 写入，`accepted` |
| 修订号高于当前 | 追加新版本并成为当前值，`accepted` |
| 修订号等于当前且值相同 | 幂等，不写新版本，`unchanged` |
| 修订号低于当前 | 整批 409 `stale_revision`，全部回滚 |
| 修订号等于当前但值不同 | 整批 409 `same_revision_different_value`，全部回滚 |
| 批内同一时刻出现不同 `(revision, value)` | 整批 409 `duplicate_hour_in_batch`，全部回滚 |
| 批内同时刻完全相同的重复条目 | 按一条处理 |

冲突响应会列出每个问题时刻的已提供/当前修订与值，便于数据员修正后重传：

```json
{
  "code": "reading_conflict",
  "message": "读数批次与已提交版本冲突，整批未写入",
  "atomic": true,
  "conflicts": [
    {
      "hour": "2026-11-16T01:00:00Z",
      "reason": "stale_revision",
      "provided_revision": 1,
      "provided_value": 120,
      "current_revision": 2,
      "current_value": 125
    }
  ]
}
```

### 结算发布

请求体：

```json
{"start_hour": "2026-11-16T00:00:00Z", "hours": 24}
```

- `hours` 取 1～48；结算覆盖 `[start_hour, start_hour + hours)`，需要 `hours + 1` 个边界整点读数。
- 每个小时区间用量 = 后值 − 前值；若后值小于前值，视为恰好一次 10000 回卷，用量 = 后值 + 10000 − 前值。
- 发布使用**同一提交状态**下每个时刻的最新修订。补录或更正会改变相邻两个区间；已经在写锁内读取的数据不会与并发录入交错。
- 缺读数或单小时用量超过 500 时返回 409，列出问题区间，**不写入任何结算数据**。缺失的连续时刻会合并为一个 `missing_reading` 区间，并同时给出逐时刻列表。
- 成功返回 201，包含结算号、总用量和每个区间的起止读数及其修订号。
- 结算明细作为快照落库：之后补录/更正/重新发布都不改变已发布结算号的内容。

### 结算变更流（数仓增量接入）

```
GET /settlement-events?after=0&limit=100
```

```json
{
  "events": [
    {
      "cursor": 1,
      "event_type": "published",
      "settlement_no": "STL-20261116T000000Z-…",
      "meter_id": "M-001",
      "start_hour": "2026-11-16T00:00:00Z",
      "end_hour": "2026-11-16T02:00:00Z",
      "hours": 2,
      "occurred_at": "2026-11-16T08:00:00.000Z"
    }
  ],
  "count": 1,
  "next_cursor": 1,
  "has_more": false
}
```

事件类型与产生时机：

| 事件 | 时机 |
| --- | --- |
| `published` | 新结算发布成功，与结算数据同一事务落库 |
| `invalidated` | 更高修订读数入库时，对**引用该时刻旧修订**的每个生效结算记一次；同值高修订同样使旧依据失效 |

消费规则与语义保证：

- 每个事件带递增游标 `cursor`；事件按数据库提交顺序返回（写入在 SQLite 写锁内串行，游标顺序即提交顺序）。
- 消费方持久化已处理的 `next_cursor`，下次用 `after=next_cursor` 继续；重试、重启后从已存游标恢复即可**不重不漏**。
- 同版同值重传是幂等操作，不产生任何事件；同一批次更正同一结算的多个边界，只记一次 `invalidated`；已撤下的结算不会被重复失效。
- 事件与业务写入同一事务提交或回滚：录入/发布失败不会出现可读取的事件。
- `invalidated` 只撤下变更流中的结算，不删除数据：原结算号仍可查冻结明细，重新发布会得到新结算号和新的 `published` 事件。
- 变更流仅覆盖功能启用后发布的结算；启用前的历史结算不产生事件，也不会被失效。

消费循环示例：

```bash
AFTER=0
while true; do
  PAGE=$(curl -s "$BASE/settlement-events?after=$AFTER&limit=100")
  # …处理 PAGE.events（published 拉取明细入库，invalidated 撤下旧结算）…
  AFTER=$(echo "$PAGE" | jq '.next_cursor')
  [ "$(echo "$PAGE" | jq '.has_more')" = "true" ] || break
done
```

失败响应示例：

```json
{
  "code": "publish_rejected",
  "message": "结算未发布：存在缺失读数或超限用量",
  "atomic": true,
  "meter_id": "M-001",
  "start_hour": "2026-11-16T00:00:00Z",
  "end_hour": "2026-11-16T03:00:00Z",
  "hours": 3,
  "problems": [
    {
      "code": "hourly_usage_exceeds_limit",
      "start_hour": "2026-11-16T00:00:00Z",
      "end_hour": "2026-11-16T01:00:00Z",
      "message": "2026-11-16T00:00:00Z 至 2026-11-16T01:00:00Z 用量 900 超过单小时上限 500",
      "usage": 900,
      "limit": 500
    },
    {
      "code": "missing_reading",
      "start_hour": "2026-11-16T02:00:00Z",
      "end_hour": "2026-11-16T03:00:00Z",
      "message": "缺少 2026-11-16T02:00:00Z 至 2026-11-16T03:00:00Z 区间内的小时读数",
      "missing_readings": ["2026-11-16T02:00:00Z"]
    }
  ]
}
```

### 完整示例

```bash
BASE=http://127.0.0.1:8000

curl -s -X POST $BASE/meters \
  -H 'Content-Type: application/json' \
  -d '{"meter_id":"M-001","name":"一号表"}'

curl -s -X POST $BASE/meters/M-001/readings \
  -H 'Content-Type: application/json' \
  -d '{"readings":[
        {"hour":"2026-11-16T00:00:00Z","value":9500,"revision":1},
        {"hour":"2026-11-16T01:00:00Z","value":9580,"revision":1},
        {"hour":"2026-11-16T02:00:00Z","value":9670,"revision":1}]}'

curl -s -X POST $BASE/meters/M-001/settlements \
  -H 'Content-Type: application/json' \
  -d '{"start_hour":"2026-11-16T00:00:00Z","hours":2}'

curl -s $BASE/settlements
```

## 并发语义

- 录入和发布都在单个 SQLite `BEGIN IMMEDIATE` 事务中完成：谁先取得写锁，谁先被裁决；后到者读到前者已提交的状态。
- 录入的版本校验与写入在同一事务内，不会出现“都校验通过、都写入”的同版双写。
- 发布在同一写锁内读取读数、校验、写结算头与明细；中途失败整体回滚，不会留下可查询的半份结算。
- 变更流事件与对应业务写入在同一事务内提交：任一失败整体回滚，不会出现“业务成功但事件丢失”或“业务失败但事件可读”。
- 读接口使用独立读事务，看到的是完整提交状态。
- SQLite 使用 WAL 模式与 30 秒 busy timeout；适用于单机本地部署，不支持多主机共享同一数据库文件。

> 约束说明：SQLite 的写锁是库级锁，因此并发写按提交顺序串行化——这正是“按提交顺序裁决”的实现方式，代价是写入吞吐受单写者限制。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

覆盖：UTC 整点/取值边界、幂等重传、旧版与同版异值整批回滚、批内同时刻冲突、回卷、500 上限边界、缺读数区间合并、发布快照不变性、发布失败无残留、多线程并发录入/发布与 HTTP 层并发一致性，以及变更流的发布/失效事件、同批多边界去重、游标分页不重不漏、失败无事件、重启后游标延续和并发不变量。
