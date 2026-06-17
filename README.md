# wms-mvp-local

Shopify 주문 웹훅을 수신하여 WMS로 이벤트를 발행하고, WMS 재고 **변동량(delta)** 을 Shopify Admin `on_hand`에 Two-Track 방식으로 역동기화하는 이벤트 기반 통합 파이프라인 MVP입니다.

호스트 PC에서 **Supabase CLI**(`supabase start`)로 PostgreSQL + Studio를 구동하고, **Docker Compose**로 앱·Kafka·Redis·Inngest·Bull-board를 구동합니다. Prisma ORM이 두 환경을 연결합니다.

## Architecture

```mermaid
flowchart LR
    subgraph host [Host Machine]
        SupabaseCLI["supabase start"]
        PG[(Supabase Postgres :54322)]
        Studio[Supabase Studio :54323]
        SupabaseCLI --> PG
        SupabaseCLI --> Studio
    end
    subgraph compose [Docker Compose]
        Shopify[Shopify Webhook] -->|POST /webhooks/shopify/orders/create| Express
        Express -->|"SET NX EX (idempotency)"| Redis[(Redis)]
        Express -->|"produce shopify.orders.created"| Kafka[(Kafka)]
        Kafka -->|consume orders| OrderConsumer[Order Kafka Consumer]
        OrderConsumer -->|"queue.add transform-order"| BullMQ[(BullMQ wms-etl)]
        OrderConsumer -->|"inngest.send order/received"| InngestSrv[Inngest Dev Server]
        BullMQ --> OrderWorker[Order BullMQ Worker]
        InngestSrv -->|HTTP /api/inngest| Workflow["orderWorkflow"]
        WMS[WMS stock change] -->|"produce wms.inventory.changes"| Kafka
        Kafka -->|consume inventory| InvConsumer[Inventory Kafka Consumer]
        InvConsumer -->|"queue.add persist-inventory-change"| InvQueue[(BullMQ inventory-change)]
        InvQueue --> InvWorker[Inventory BullMQ Worker]
        InvWorker -->|"create delta + increment stock"| PG
        InvWorker -->|"stock <= threshold"| InngestSrv
        InngestSrv -->|"cron */15m | event inventory/sync.requested"| Track1["inventorySync Track1"]
        InngestSrv -->|"event inventory/critical.detected"| Track2["inventoryCriticalSync Track2"]
        BullBoard[Bull-board /admin/queues] -.observes.-> BullMQ
        BullBoard -.observes.-> InvQueue
    end
    Shop[(Shopify Admin GraphQL)]
    OrderWorker -->|"prisma.wmsOrder.create PENDING"| PG
    Workflow -->|"prisma.wmsOrder.update READY_TO_SHIP"| PG
    Track1 --> SVC["pushPendingDeltas()"]
    Track2 --> SVC
    SVC -->|"inventorySetQuantities on_hand"| Shop
    SVC -->|"isShopifyPushed=true"| PG
    Studio -.observes.-> PG
```

### Order lifecycle

1. Shopify 웹훅 수신 → Redis 멱등성 키 저장 → Kafka `shopify.orders.created` 토픽 발행
2. Kafka Consumer → BullMQ `wms-etl` 큐 + Inngest `order/received` 이벤트 동시 디스패치
3. BullMQ Worker → Prisma `wmsOrder.create()` → Supabase DB에 `PENDING` INSERT
4. Inngest `orderWorkflow` → 10초 대기 → 검증 → `finalize-wms-allocation` → `READY_TO_SHIP` UPDATE

### Inventory sync lifecycle (Two-Track)

WMS 내부는 **변동량(Delta) 큐**로 관리하고, Shopify push는 **`2026-04` `inventorySetQuantities`**로 `on_hand`를 갱신합니다. 대량 마스터 변경은 **10개/청크 순차 push + partial success**로 fault를 격리합니다. 위험도에 따라 두 트랙으로 분리됩니다.

#### Intake: WMS 재고 변동 수신

1. WMS(또는 테스트 스크립트)가 Kafka `wms.inventory.changes` 토픽에 메시지 발행
   - Payload: `{ "sku": "...", "locationName": "...", "delta": -5 }` (`delta` 양수=입고, 음수=출고)
2. Inventory Kafka Consumer → BullMQ `inventory-change` 큐 enqueue
3. Inventory BullMQ Worker (단일 트랜잭션):
   - `wms_inventory_changes`에 delta 행 INSERT (`isShopifyPushed = false`)
   - `WmsInventory.stock`을 `increment: delta`로 갱신 (스냅샷)
4. 갱신 후 `stock <= CRITICAL_STOCK_THRESHOLD`(기본 `0`)이면 Inngest `inventory/critical.detected` 이벤트 즉시 발행

#### Track 1: Bulk Batch Sync (일반 변동)

- **트리거**: Inngest cron `*/15 * * * *` 또는 수동 이벤트 `inventory/sync.requested`
- **함수**: `inventorySync` → `pushPendingDeltas()` (전체 미처리 delta)
- **Shopify API**: `inventorySetQuantities` (`name: "on_hand"`, `@idempotent`, `changeFromQuantity`, **10개/청크 순차**)
- **완료**: 성공 청크 source 행만 `isShopifyPushed = true` (partial success)

#### Track 2: Instant Single-Shot Sync (품절 임박 방어)

- **트리거**: `inventory/critical.detected` (BullMQ worker가 임계값 진입 시 자동 발행)
- **함수**: `inventoryCriticalSync` → `pushPendingDeltas({ sku })` (해당 SKU 미처리 delta만 즉시 drain)
- **Shopify API**: Track 1과 동일한 `pushPendingDeltas()` 코드 경로

#### 공유 서비스: `pushPendingDeltas()`

[`src/services/shopifyInventoryAdjust.ts`](src/services/shopifyInventoryAdjust.ts)가 양 트랙의 단일 push 경로입니다.

1. `wms_inventory_changes`에서 `isShopifyPushed = false` 행 조회 (Track 2는 SKU 필터, 최대 500행)
2. `sku + locationName` 기준으로 `deltaQty` 합산 (zero-sum 그룹 제외)
3. Shopify ID 해석 — **벌크 GraphQL** (SKU/location 수에 비례한 N+1 호출 없음):
   - `locations(first: 50)` — location 이름 → ID (1회)
   - `productVariants(first: N, query: "sku:'A' OR sku:'B' OR ...")` — SKU → `inventoryItem.id` (50 SKU/청크, 청크 병렬)
4. 현재 `on_hand` 조회 — **벌크 GraphQL**:
   - `nodes(ids: [inventoryItemId...])` + `... on InventoryItem { inventoryLevels { ... } }` (100 item/청크, 청크 병렬)
   - 요청된 `(inventoryItemId, locationId)` 쌍만 메모리에서 필터링 → `changeFromQuantity`
5. `quantity = changeFromQuantity + delta` 계산 후 **`inventorySetQuantities`를 10개 단위 청크로 순차 push** (`MUTATION_CHUNK_SIZE = 10`)
   - 청크별 `@idempotent` 키: 해당 청크 `sourceIds` SHA256 해시
   - **Fault isolation**: 한 청크의 `userErrors`/`CHANGE_FROM_QUANTITY_STALE`이 다른 청크에 전파되지 않음
6. **Partial success**: 성공 청크의 source 행만 `isShopifyPushed = true` 마킹. 실패 청크는 `false` 유지 → 다음 cron에서 재시도

**GraphQL 호출 수 (대표 케이스)**

| 단계 | 쿼리 | 호출 횟수 (예: SKU 30개) |
|------|------|--------------------------|
| Location resolve | `locations` | 1 |
| Variant resolve | `productVariants(query: OR...)` | 1 (≤50 SKU) |
| on_hand read | `nodes(ids:)` | 1 (≤100 item) |
| Push | `inventorySetQuantities` | 3 (30 SKU ÷ 10, **순차**) |
| **합계** | | **6회** (조회 3 + push 3) |

**반환값 (`PushPendingDeltasResult`)**

| 필드 | 설명 |
|------|------|
| `pushed` | 성공한 aggregated change 수 (SKU+location 그룹) |
| `attempted` | 조회된 source row 수 |
| `failed` | 실패한 source row 수 |
| `failedReasons` | 청크별 `{ sourceIds, message, code? }` — Inngest 로그에서 확인 |
| `skipped` / `reason` | `no-pending-deltas`, `no-resolvable-deltas` 등 |

> **2026-04 API 참고**: `inventoryAdjustQuantities`는 `on_hand`를 지원하지 않습니다 (`available`, `damaged` 등 ledger 타입만 가능). `on_hand` 갱신은 `inventorySetQuantities`를 사용하며, `changeFromQuantity` + `@idempotent` directive가 **필수**입니다. WMS delta는 절대값으로 변환해 push합니다 (`quantity = 현재 on_hand + delta`).
>
> **벌크 조회 한계**: SKU 청크 50개, inventory item 청크 100개. `inventoryLevels(first: 50)`은 item당 location 50개 이하 MVP 가정(단일 `Shop location` 환경). SKU에 `'` 포함 시 검색 쿼리 이스케이프 처리.
>
> **마스터 파일 배치 / Fault isolation**: 500개 delta를 단일 mutation으로 처리하지 않습니다. `MUTATION_CHUNK_SIZE = 10` 순차 루프로 Rate Limit·레이스 컨디션 영향을 격리합니다. `failed > 0`이어도 함수는 정상 종료(Inngest step retry 없음). 실패 row는 다음 `*/15` cron에서 자동 재집계됩니다.
>
> **동시성**: `changeFromQuantity` stale 시 해당 **청크만** `failedReasons`에 기록되고 나머지 청크는 push 계속됩니다.
>
> **`WmsInventory.isSynced`**: 레거시 컬럼으로 유지되나, 현재 sync 파이프라인은 `wms_inventory_changes` 큐를 사용합니다.

### DB connectivity

| From | Connection target | `DATABASE_URL` |
|------|-------------------|----------------|
| Host (로컬 dev) | `127.0.0.1:54322` | `postgresql://postgres:postgres@127.0.0.1:54322/postgres` |
| Docker `app` container | `host.docker.internal:54322` | `postgresql://postgres:postgres@host.docker.internal:54322/postgres` |

`app` 서비스는 `extra_hosts: host.docker.internal:host-gateway`로 Linux 환경에서도 호스트 DB에 접근합니다.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Runtime | Node.js 20 (Alpine) |
| Language | TypeScript |
| Web Framework | Express |
| Message Broker | Apache Kafka (kafkajs) |
| Task Queue | BullMQ (ioredis) |
| Workflow | Inngest |
| Database | Supabase CLI (PostgreSQL on host `:54322`) |
| ORM | Prisma |
| DB UI | Supabase Studio (`:54323`) |
| Monitoring | Bull-board, Inngest Dev Server |

## Services Reference (서비스별 역할 및 설정)

시스템은 **호스트 Supabase CLI**와 **Docker Compose** 두 레이어로 구성됩니다. 아래 표는 각 서비스의 역할, 접속 정보, 주요 설정값을 정리한 것입니다.

### Overview

| Service | Layer | Port (host) | Role in pipeline |
|---------|-------|-------------|------------------|
| Supabase PostgreSQL | Host (CLI) | 54322 | WMS 영구 저장 (`WmsOrder`, `WmsInventory`, `wms_inventory_changes`) |
| Supabase Studio | Host (CLI) | 54323 | DB 테이블 조회·SQL 실행 UI |
| `app` | Docker | 3000 | 웹훅 수신, Kafka Producer/2×Consumer, 2×BullMQ Worker, Prisma, Inngest serve |
| `redis` | Docker | 6379 | 웹훅 멱등성 키 + BullMQ 큐 백엔드 (`wms-etl`, `inventory-change`) |
| `zookeeper` | Docker | — | Kafka 메타데이터 코디네이션 |
| `kafka` | Docker | 9094 | 이벤트 브로커 (`shopify.orders.created`, `wms.inventory.changes`) |
| `inngest-dev` | Docker | 8288, 8289 | 워크플로우 오케스트레이션 Dev Server + UI |

---

### Host: Supabase CLI (`supabase start`)

Supabase CLI는 Docker 위에서 로컬 Supabase 스택(PostgreSQL, Studio, Auth, API 등)을 기동합니다. **이 MVP는 PostgreSQL과 Studio만 파이프라인에 사용**합니다.

설정 파일: [`supabase/config.toml`](supabase/config.toml)

#### Supabase PostgreSQL

| Item | Value |
|------|-------|
| **역할** | Prisma ORM이 접근하는 영구 저장소. BullMQ가 `PENDING` INSERT, Inngest가 `READY_TO_SHIP` UPDATE |
| **Port** | `54322` (config: `[db].port`) |
| **Connection URL** | `postgresql://postgres:postgres@127.0.0.1:54322/postgres` |
| **From Docker `app`** | `postgresql://postgres:postgres@host.docker.internal:54322/postgres` |
| **User / Password** | `postgres` / `postgres` (Supabase CLI 기본값) |
| **DB version** | PostgreSQL 17 (`[db].major_version = 17`) |
| **Schema sync** | 앱 시작 시 `npx prisma db push` → `public.WmsOrder`, `public.WmsInventory`, `public.wms_inventory_changes` 생성 |

#### Supabase Studio (Dashboard)

| Item | Value |
|------|-------|
| **역할** | DB 대시보드 — Table Editor, SQL Editor, 스키마 확인 |
| **URL** | http://localhost:54323 (config: `[studio].port = 54323`) |
| **기동 방법** | `supabase start` (Compose에 포함되지 않음) |
| **로그인** | 로컬 Dev — 별도 계정 없이 바로 접속 |

**주요 메뉴**

| Menu | 용도 |
|------|------|
| **Table Editor** | `WmsOrder`, `WmsInventory`, `wms_inventory_changes` 행 조회·수정·INSERT (GUI) |
| **SQL Editor** | 아래 검증/시드 SQL 실행, `Run` 또는 `Ctrl+Enter` |
| **Database** → **Tables** | Prisma `db push`로 생성된 테이블 목록 확인 |

**Studio에서 확인할 테이블**

| Table | 시점 | Expected |
|-------|------|----------|
| `WmsOrder` | BullMQ ETL 직후 (~2s) | `status = PENDING` |
| `WmsOrder` | Inngest workflow 완료 (~10s) | `status = READY_TO_SHIP` |
| `WmsInventory` | inventory change worker 직후 | `stock`이 delta만큼 증감 |
| `wms_inventory_changes` | Kafka 메시지 수신 직후 | `isShopifyPushed = false`, `deltaQty` 기록 |
| `wms_inventory_changes` | Shopify push 성공 후 | `isShopifyPushed = true` |

**Table Editor — `WmsInventory` 스냅샷 시드 (테스트 전제)**

Kafka delta intake는 **기존 SKU 행**이 `WmsInventory`에 있어야 합니다 (`update where: { sku }`).

1. http://localhost:54323 → **Table Editor** → `WmsInventory` 선택
2. **Insert row** 클릭
3. 필드 입력:
   - `sku`: Shopify variant SKU (예: `sku-managed-1`)
   - `stock`: 초기 스냅샷 수량 (예: `100`)
   - `location`: 참고용 location 이름 (예: `Shop location`) — delta push는 `locationName`을 Kafka payload에서 사용
   - `isSynced`: `true` (레거시 컬럼, 현재 파이프라인 미사용)
4. **Save** → 이후 Kafka로 delta 메시지 발행

```sql
-- SQL Editor 예시: 주문 조회
SELECT id, "shopifyId", status, "createdAt", "updatedAt"
FROM "WmsOrder"
ORDER BY id DESC;

-- SQL Editor 예시: 재고 스냅샷 시드 (Kafka delta 테스트 전제)
INSERT INTO "WmsInventory" ("sku", "stock", "location", "isSynced", "updatedAt")
VALUES ('sku-managed-1', 100, 'Shop location', true, NOW())
ON CONFLICT ("sku") DO UPDATE
SET "stock" = 100, "location" = 'Shop location', "updatedAt" = NOW();

-- SQL Editor 예시: delta 큐 상태 확인
SELECT id, sku, "locationName", "deltaQty", "isShopifyPushed", "createdAt"
FROM "wms_inventory_changes"
ORDER BY id DESC;
```

#### 기타 Supabase CLI 서비스 (참고)

| Service | Port | Note |
|---------|------|------|
| Supabase API (Kong) | 54321 | REST/GraphQL — **이 MVP 앱은 직접 사용하지 않음** |
| Inbucket (email test) | 54324 | Auth 이메일 테스트용 |
| Shadow DB | 54320 | `supabase db diff` 전용 |

---

### Docker Compose: `app`

| Item | Value |
|------|-------|
| **역할** | 파이프라인 핵심 프로세스 — Express API, Kafka Producer/Consumer, BullMQ Worker, Prisma Client, Inngest serve, Bull-board |
| **Image** | [`Dockerfile`](Dockerfile) 멀티스테이지 빌드 (`node:20-alpine`) |
| **Port** | `3000:3000` |
| **Startup command** | `npx prisma db push --skip-generate && node dist/index.js` |
| **Healthcheck** | `GET /health` (wget, 10s interval) |
| **depends_on** | `redis`, `kafka` (healthcheck 통과 후 기동) |

**환경변수 (`docker-compose.yml`)**

| Variable | Value | 설명 |
|----------|-------|------|
| `PORT` | `3000` | HTTP 서버 포트 |
| `KAFKA_BROKER` | `kafka:9092` | Docker 네트워크 내 Kafka broker |
| `REDIS_URL` | `redis://redis:6379` | Redis 연결 (멱등성 + BullMQ) |
| `DATABASE_URL` | `...@host.docker.internal:54322/postgres` | 호스트 Supabase DB |
| `INNGEST_DEV` | `1` | Inngest 로컬 Dev 모드 |
| `INNGEST_BASE_URL` | `http://inngest-dev:8288` | Inngest Dev Server 주소 |
| `INNGEST_SERVE_HOST` | `http://app:3000` | Inngest가 콜백할 앱 URL (Docker 내부) |
| `INNGEST_EVENT_KEY` | `local-dev-event-key` | 이벤트 전송 키 |
| `INNGEST_SIGNING_KEY` | `local-dev-signing-key` | 서명 검증 키 |
| `SHOPIFY_SHOP_DOMAIN` | `${SHOPIFY_SHOP_DOMAIN:-}` | Shopify 스토어 도메인 (`*.myshopify.com`) — `inventorySync`용 |
| `SHOPIFY_ADMIN_ACCESS_TOKEN` | `${SHOPIFY_ADMIN_ACCESS_TOKEN:-}` | Admin API access token (custom app 발급) |
| `SHOPIFY_API_VERSION` | `${SHOPIFY_API_VERSION:-2026-04}` | Admin GraphQL API 버전 (`.env`로 `2026-04` 등 override) |
| `CRITICAL_STOCK_THRESHOLD` | `${CRITICAL_STOCK_THRESHOLD:-0}` | Track 2 즉시 sync 트리거 임계값 (`stock <= threshold`) |

> Shopify 변수는 **호스트 `.env`**에서 읽혀 `docker-compose up` 시 `app` 컨테이너로 전달됩니다. `.env` 변경 후 `docker-compose up -d --force-recreate app`으로 재기동하세요.

**Docker 네트워크 설정**

| Setting | Value | 설명 |
|---------|-------|------|
| `extra_hosts` | `host.docker.internal:host-gateway` | 컨테이너 → 호스트 Supabase DB(`54322`) 접근 |

**`app` 내부 컴포넌트**

| Component | Source | Role |
|-----------|--------|------|
| Express API | `src/api/server.ts` | 웹훅 `/webhooks/shopify/orders/create`, `/health` |
| Kafka Producer | `src/config/kafka.ts` | 웹훅 수신 → `shopify.orders.created` 발행 |
| Order Kafka Consumer | `src/index.ts` | `shopify.orders.created` consume → BullMQ + Inngest 디스패치 |
| Inventory Kafka Consumer | `src/index.ts` | `wms.inventory.changes` consume → `inventory-change` 큐 enqueue |
| Order BullMQ Worker | `src/workers/bullmq/worker.ts` | ETL + `prisma.wmsOrder.create()` |
| Inventory BullMQ Worker | `src/workers/bullmq/inventoryChangeWorker.ts` | delta INSERT + stock increment + critical event |
| Shopify adjust service | `src/services/shopifyInventoryAdjust.ts` | `pushPendingDeltas()` — bulk resolve + chunked `inventorySetQuantities` (10/chunk, fault isolation) |
| Inngest serve | `src/api/server.ts` | `/api/inngest` — `orderWorkflow`, `inventorySync`, `inventoryCriticalSync` |
| Bull-board | `src/api/server.ts` | `/admin/queues` — `wms-etl`, `inventory-change` 모니터링 |
| Prisma Client | `src/config/prisma.ts` | Supabase PostgreSQL ORM |

---

### Docker Compose: `redis`

| Item | Value |
|------|-------|
| **역할** | ① 웹훅 멱등성 (`SET key NX EX 86400`) ② BullMQ job queue/state 저장 |
| **Image** | `redis:7-alpine` |
| **Port** | `6379:6379` |
| **App connection** | `REDIS_URL=redis://redis:6379` |
| **Healthcheck** | `redis-cli ping` (5s interval, 10 retries) |

**사용처**

| Feature | Key pattern | TTL |
|---------|-------------|-----|
| Webhook idempotency | `idemp:shopify:{webhook-id}` | 86400s (24h) |
| BullMQ queue | `wms-etl`, `inventory-change` (internal BullMQ keys) | job lifecycle |

**ioredis 설정** (`src/config/redis.ts`): `maxRetriesPerRequest: null` (BullMQ 필수)

---

### Docker Compose: `zookeeper`

| Item | Value |
|------|-------|
| **역할** | Kafka broker 메타데이터·리더 선출 관리 (ZooKeeper 모드) |
| **Image** | `confluentinc/cp-zookeeper:7.5.0` |
| **Port** | 내부 전용 (호스트 미노출) |
| **Healthcheck** | `echo srvr \| nc localhost 2181` → `Mode:` 응답 확인 |

**환경변수**

| Variable | Value | 설명 |
|----------|-------|------|
| `ZOOKEEPER_CLIENT_PORT` | `2181` | ZooKeeper 클라이언트 포트 |
| `ZOOKEEPER_TICK_TIME` | `2000` | 세션 heartbeat tick (ms) |

---

### Docker Compose: `kafka`

| Item | Value |
|------|-------|
| **역할** | Shopify 주문·WMS 재고 변동 이벤트 브로커 — 웹훅/Producer와 Consumer 사이 decoupling |
| **Image** | `confluentinc/cp-kafka:7.5.0` |
| **Port (host)** | `9094` (`PLAINTEXT_HOST`) |
| **Port (Docker network)** | `9092` (`PLAINTEXT`, broker 간 통신) |
| **depends_on** | `zookeeper` (healthcheck 통과 후 기동) |
| **Healthcheck** | `kafka-broker-api-versions --bootstrap-server localhost:9092` |

**환경변수**

| Variable | Value | 설명 |
|----------|-------|------|
| `KAFKA_BROKER_ID` | `1` | 단일 broker ID |
| `KAFKA_ZOOKEEPER_CONNECT` | `zookeeper:2181` | ZooKeeper 연결 |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:9092`, `PLAINTEXT_HOST://localhost:9094` | Docker/호스트 각각 다른 listener |
| `KAFKA_LISTENERS` | `PLAINTEXT://0.0.0.0:9092`, `PLAINTEXT_HOST://0.0.0.0:9094` | 바인딩 주소 |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `PLAINTEXT` | broker 간 통신 listener |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | 단일 노드 MVP |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` | 첫 produce 시 토픽 자동 생성 |

**앱 코드 설정** (`src/config/kafka.ts`)

| Setting | Value |
|---------|-------|
| Order topic | `shopify.orders.created` |
| Order consumer group | `wms-order-consumer` |
| Inventory topic | `wms.inventory.changes` |
| Inventory consumer group | `wms-inventory-consumer` |
| Client ID | `wms-mvp-local` (env: `KAFKA_CLIENT_ID`) |
| Broker (in app container) | `kafka:9092` |
| Broker (호스트 스크립트) | `localhost:9094` (`PLAINTEXT_HOST`, 포트 `9094:9094` 매핑) |

> **호스트 vs Docker**: `docker-compose.yml`의 `app`은 `KAFKA_BROKER=kafka:9092`로 고정됩니다. 호스트에서 `npm run test:inventory-change` 등 Kafka producer를 실행할 때는 `.env`에 `KAFKA_BROKER=localhost:9094`가 필요합니다 (`kafka` hostname은 Docker 네트워크 내부 전용).

---

### Docker Compose: `inngest-dev`

| Item | Value |
|------|-------|
| **역할** | Inngest Dev Server — 이벤트 수신, `orderWorkflow` 스케줄/실행, step 상태 관리, 디버그 UI |
| **Image** | `inngest/inngest:latest` |
| **Port** | `8288` (API + Dashboard), `8289` (Connect WebSocket) |
| **depends_on** | `app` ( `/api/inngest` 폴링 대상) |

**Command**

```yaml
command: ["inngest", "dev", "-u", "http://app:3000/api/inngest"]
```

| Setting | Value | 설명 |
|---------|-------|------|
| `-u` URL | `http://app:3000/api/inngest` | Docker 네트워크 내 앱 Inngest serve 엔드포인트 |
| Dashboard | http://localhost:8288 | 함수 실행·step 로그 확인 |

**앱 ↔ Inngest 연동**

| App env | Value | Direction |
|---------|-------|-----------|
| `INNGEST_BASE_URL` | `http://inngest-dev:8288` | app → Inngest (이벤트 전송) |
| `INNGEST_SERVE_HOST` | `http://app:3000` | Inngest → app (함수 invoke 콜백) |

**`orderWorkflow` steps** (`src/workers/inngest/client.ts`)

| Step | Action |
|------|--------|
| `wait-validation` | `step.sleep("10s")` |
| `validate` | order ID 존재 확인 |
| `finalize-wms-allocation` | `prisma.wmsOrder.update` → `READY_TO_SHIP` |

**`inventorySync` (Track 1)** — [`src/workers/inngest/inventorySync.ts`](src/workers/inngest/inventorySync.ts)

| Trigger | Value |
|---------|-------|
| Cron | `*/15 * * * *` (15분마다) |
| Event | `inventory/sync.requested` (수동/외부 호출) |

| Step | Action |
|------|--------|
| `push-pending-deltas` | `pushPendingDeltas()` — 전체 미처리 delta를 Shopify에 일괄 push |

**`inventoryCriticalSync` (Track 2)** — [`src/workers/inngest/inventoryCriticalSync.ts`](src/workers/inngest/inventoryCriticalSync.ts)

| Trigger | Value |
|---------|-------|
| Event | `inventory/critical.detected` (BullMQ worker가 `stock <= CRITICAL_STOCK_THRESHOLD` 시 자동 발행) |

| Step | Action |
|------|--------|
| `push-critical` | `pushPendingDeltas({ sku })` — 해당 SKU 미처리 delta 즉시 drain |

**`pushPendingDeltas()` 내부** — [`src/services/shopifyInventoryAdjust.ts`](src/services/shopifyInventoryAdjust.ts)

| Phase | Action |
|-------|--------|
| Fetch | `wms_inventory_changes` where `isShopifyPushed = false` (optional SKU filter, `take: 500`) |
| Aggregate | `sku + locationName` 기준 `deltaQty` 합산 |
| Resolve locations | `locations(first: 50)` — 1회, 이름 → `locationId` |
| Resolve variants | `productVariants(first: N, query: "sku:'A' OR sku:'B' ...")` — 50 SKU/청크, `Promise.all` 병렬 |
| Read on_hand | `nodes(ids: [inventoryItemId...])` + `inventoryLevels(first: 50)` — 100 item/청크, `wantedKeys`로 location 필터 |
| Push | `inventorySetQuantities` — **10개/청크 순차** (`MUTATION_CHUNK_SIZE`), 청크별 `@idempotent` |
| Mark | 성공 청크 source 행만 `isShopifyPushed = true` (partial success) |
| Result | `{ pushed, attempted, failed, failedReasons, skipped, reason? }` |

**벌크 resolve 쿼리 예시** (개념):

```graphql
# SKU → inventoryItem.id (청크당 1회)
query($q: String!, $first: Int!) {
  productVariants(first: $first, query: $q) {
    nodes { sku inventoryItem { id } }
  }
}
# variables: { "q": "sku:'SKU-A' OR sku:'SKU-B'", "first": 2 }

# on_hand 조회 (청크당 1회)
query($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on InventoryItem {
      id
      inventoryLevels(first: 50) {
        nodes {
          location { id }
          quantities(names: ["on_hand"]) { name quantity }
        }
      }
    }
  }
}
```

**2026-04 mutation 예시** (개념):

```graphql
mutation($input: InventorySetQuantitiesInput!, $idempotencyKey: String!) {
  inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
    inventoryAdjustmentGroup { createdAt reason }
    userErrors { field message code }
  }
}
```

```json
{
  "input": {
    "name": "on_hand",
    "reason": "correction",
    "quantities": [
      {
        "inventoryItemId": "gid://shopify/InventoryItem/...",
        "locationId": "gid://shopify/Location/...",
        "quantity": 52,
        "changeFromQuantity": 50
      }
    ]
  },
  "idempotencyKey": "<sha256 of chunk source row IDs, max 10 per call>"
}
```

> **GraphQL API**: `SHOPIFY_API_VERSION` 기본 `2026-04`. `inventorySetQuantities`에 `changeFromQuantity`와 `@idempotent`가 필수입니다. 구 `compareQuantity` / `ignoreCompareQuantity` 필드는 2026-04에서 제거되었습니다.
> **필수 환경변수**: `SHOPIFY_SHOP_DOMAIN`, `SHOPIFY_ADMIN_ACCESS_TOKEN`.
> **Early return**: `{ skipped: true, reason: "no-pending-deltas", failed: 0, failedReasons: [] }` 또는 `{ skipped: true, reason: "no-resolvable-deltas", ... }`.
> **Partial failure**: `{ skipped: false, pushed: N, failed: M, failedReasons: [...] }` — Inngest step은 성공으로 종료. `failed > 0` row는 다음 cron에서 재push.
> **코드 변경 후**: `docker compose build app`로 이미지 재빌드 필요 (`dist/`는 Dockerfile build stage에서 생성).

---

### Embedded: BullMQ + Bull-board

별도 컨테이너 없이 `app` 프로세스 내부에서 동작합니다.

#### Queue: `wms-etl` (주문 ETL)

| Item | Value |
|------|-------|
| **역할** | Shopify → WMS 주문 ETL 비동기 처리 |
| **Job name** | `transform-order` |
| **Worker** | `src/workers/bullmq/worker.ts` |
| **DB write** | `prisma.wmsOrder.create({ shopifyId, status: "PENDING" })` |

#### Queue: `inventory-change` (재고 delta intake)

| Item | Value |
|------|-------|
| **역할** | Kafka `wms.inventory.changes` 메시지를 DB delta 큐 + 스냅샷에 반영 |
| **Job name** | `persist-inventory-change` |
| **Worker** | `src/workers/bullmq/inventoryChangeWorker.ts` |
| **Payload** | `{ sku, locationName, delta }` |
| **DB write** | `wms_inventory_changes` INSERT + `WmsInventory.stock` increment |
| **Side effect** | `stock <= CRITICAL_STOCK_THRESHOLD` → `inventory/critical.detected` |

#### Bull-board (공통)

| Item | Value |
|------|-------|
| **Backend** | Redis (`REDIS_URL`) |
| **Dashboard** | http://localhost:3000/admin/queues |
| **Queues** | `wms-etl`, `inventory-change` |

---

### Embedded: Prisma ORM

| Item | Value |
|------|-------|
| **역할** | Type-safe DB access — schema 정의, migrate/push, query |
| **Schema** | [`prisma/schema.prisma`](prisma/schema.prisma) |
| **Client** | [`src/config/prisma.ts`](src/config/prisma.ts) (singleton) |
| **Packages** | `@prisma/client` (runtime), `prisma` (dev + container CLI) |
| **Sync on startup** | `npx prisma db push --skip-generate` (app container command) |
| **Bootstrap log** | `[prisma] database connected` (`src/index.ts`) |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- [Supabase CLI](https://supabase.com/docs/guides/cli)

### Run

```bash
# 0. (최초 1회) 환경변수 설정
cp .env.example .env
# .env 편집: DATABASE_URL, SHOPIFY_* (inventory sync 사용 시)

# 0b. (inventory sync) Admin API 토큰 발급 — Partner app OAuth
npm run shopify:token
# 출력된 SHOPIFY_ADMIN_ACCESS_TOKEN= 를 .env에 붙여넣기

# 1. 호스트에서 Supabase 로컬 스택 기동 (PostgreSQL + Studio)
supabase start

# 2. Docker 파이프라인 기동 (.env의 SHOPIFY_* 가 app 컨테이너로 전달됨)
docker-compose up --build
```

`supabase start` 출력에서 DB URL과 Studio URL을 확인합니다:

```
Database URL: postgresql://postgres:postgres@127.0.0.1:54322/postgres
Studio URL:   http://127.0.0.1:54323
```

앱 컨테이너 startup logs:

```
🚀  Your database is now in sync with your Prisma schema.
[prisma] database connected
[kafka] consumer started
[kafka] inventory change consumer started
[bullmq] workers started
[app] ready on :3000
```

> **Important:** `supabase start`를 먼저 실행하지 않으면 `app` 컨테이너가 `P1001: Can't reach database server at host.docker.internal:54322`로 실패합니다.

### URLs

| URL | Description |
|-----|-------------|
| http://localhost:3000/health | Health check |
| http://localhost:3000/admin/queues | Bull-board queue dashboard |
| http://localhost:54323 | Supabase Studio (Table Editor, SQL Editor) |
| http://localhost:8288 | Inngest Dev Server UI |

> 각 서비스의 역할·포트·환경변수 상세는 [Services Reference](#services-reference-서비스별-역할-및-설정) 참고.

## API Reference

### `GET /health`

Health check endpoint used by Docker healthcheck.

**Response `200`**

```json
{ "status": "ok" }
```

---

### `POST /webhooks/shopify/orders/create`

Shopify order creation webhook receiver.

**Headers**

| Header | Required | Description |
|--------|----------|-------------|
| `x-shopify-webhook-id` | Recommended | Idempotency key source |
| `content-type` | Yes | `application/json` |

**Request body** — Shopify order JSON (minimum: `{ "id": number, "line_items": [] }`)

**Responses**

| Status | Body | Condition |
|--------|------|-----------|
| `202` | `{ "accepted": true }` | New webhook, published to Kafka |
| `200` | `{ "duplicated": true }` | Duplicate webhook (Redis `SET NX EX`, TTL 24h) |
| `500` | `{ "error": "Failed to process webhook" }` | Internal error |

**Example (PowerShell)**

```powershell
$body = '{"id":1001,"line_items":[]}'
Invoke-RestMethod `
  -Uri http://localhost:3000/webhooks/shopify/orders/create `
  -Method POST `
  -Headers @{ "x-shopify-webhook-id" = "t1"; "content-type" = "application/json" } `
  -Body $body
```

---

### `ALL /api/inngest`

Inngest SDK serve endpoint. Inngest Dev Server가 함수를 sync/invoke합니다.

| Function | Trigger | Role |
|----------|---------|------|
| `orderWorkflow` | `order/received` | 주문 검증 → `READY_TO_SHIP` UPDATE |
| `inventorySync` | cron `*/15 * * * *`, `inventory/sync.requested` | Track 1: 미처리 delta bulk push |
| `inventoryCriticalSync` | `inventory/critical.detected` | Track 2: critical SKU delta 즉시 push |

---

### `GET /admin/queues`

Bull-board UI for monitoring BullMQ queues (`wms-etl`, `inventory-change`). 상세 설정은 [Embedded: BullMQ + Bull-board](#embedded-bullmq--bull-board) 참고.

---

### Kafka Topics (internal)

HTTP 엔드포인트가 아닌 Kafka 토픽으로 이벤트를 수신합니다.

#### `shopify.orders.created`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | number \| string | Yes | Shopify order ID |
| `line_items` | array | No | Order line items |

Producer: `POST /webhooks/shopify/orders/create` → Kafka Producer

#### `wms.inventory.changes`

WMS 재고 변동 intake. 첫 produce 시 토픽 자동 생성 (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sku` | string | Yes | Shopify variant SKU (`WmsInventory`에 사전 존재 필요) |
| `locationName` | string | Yes | Shopify Location 이름 (delta push 매핑 키) |
| `delta` | number | Yes | 변동량 (양수=입고, 음수=출고) |

**Example message**

```json
{ "sku": "TEST-SKU-001", "locationName": "Shop location", "delta": -5 }
```

Consumer: Inventory Kafka Consumer → BullMQ `inventory-change` → [`inventoryChangeWorker.ts`](src/workers/bullmq/inventoryChangeWorker.ts)

## Database Schema

Managed by Prisma ([`prisma/schema.prisma`](prisma/schema.prisma)). Auto-synced on app container startup via `npx prisma db push --skip-generate`.

### `WmsOrder`

| Column | Type | Notes |
|--------|------|-------|
| `id` | Int | PK, auto-increment |
| `shopifyId` | String | Unique, Shopify order ID |
| `status` | String | Default `PENDING` → `READY_TO_SHIP` |
| `createdAt` | DateTime | Auto |
| `updatedAt` | DateTime | Auto |

### `WmsInventory`

WMS 재고 **스냅샷** 테이블. Kafka delta intake 시 `stock`이 increment됩니다.

| Column | Type | Notes |
|--------|------|-------|
| `id` | Int | PK, auto-increment |
| `sku` | String | Unique. Shopify variant SKU와 매칭 |
| `stock` | Int | WMS 실재고 스냅샷 (delta intake 시 `increment`) |
| `location` | String | 참고용 location 이름 (delta push는 `wms_inventory_changes.locationName` 사용) |
| `isSynced` | Boolean | **레거시** — 현재 파이프라인 미사용 (`wms_inventory_changes` 큐 사용) |
| `createdAt` | DateTime | Auto |
| `updatedAt` | DateTime | Auto |

### `wms_inventory_changes` (`WmsInventoryChange`)

Shopify로 push할 **변동량 큐**. Prisma model: `WmsInventoryChange`, DB table: `wms_inventory_changes`.

| Column | Type | Notes |
|--------|------|-------|
| `id` | BigInt | PK, auto-increment |
| `sku` | String | Shopify variant SKU |
| `locationName` | String | Shopify Location **이름**과 일치해야 함 |
| `deltaQty` | Int | 변동량 (양수=입고, 음수=출고) |
| `isShopifyPushed` | Boolean | Default `false`. push 성공 후 `true` |
| `createdAt` | DateTime | Auto |

**Index**: `(isShopifyPushed, sku)` — Track 1/2 조회 최적화

### Query orders (CLI)

Supabase Studio 사용을 권장합니다. CLI로 조회할 경우:

```powershell
echo 'SELECT id, "shopifyId", status FROM "WmsOrder";' |
  docker run -i --rm postgres:15-alpine `
    psql "postgresql://postgres:postgres@host.docker.internal:54322/postgres"
```

또는 Supabase CLI:

```bash
supabase db execute --sql 'SELECT id, "shopifyId", status FROM "WmsOrder";'
```

## Environment Variables

전체 변수 설명은 [Services Reference](#services-reference-서비스별-역할-및-설정)의 각 서비스 표를 참고하세요. 템플릿: [`.env.example`](.env.example).

| Variable | Default (Docker `app`) | Default (host dev) | Used by |
|----------|------------------------|--------------------|---------|
| `PORT` | `3000` | `3000` | Express |
| `KAFKA_BROKER` | `kafka:9092` (compose 고정) | `localhost:9094` (호스트 producer/consumer) | kafkajs Producer/Consumer. `.env`의 `kafka:9092`는 Docker `app`용; 호스트 테스트 스크립트는 `9094` |
| `REDIS_URL` | `redis://redis:6379` | `redis://localhost:6379` | ioredis, BullMQ |
| `DATABASE_URL` | `...@host.docker.internal:54322/postgres` | `...@127.0.0.1:54322/postgres` | Prisma |
| `INNGEST_DEV` | `1` | `1` | Inngest SDK |
| `INNGEST_BASE_URL` | `http://inngest-dev:8288` | `http://localhost:8288` | Inngest SDK → Dev Server |
| `INNGEST_SERVE_HOST` | `http://app:3000` | `http://localhost:3000` | Inngest Dev Server → app |
| `INNGEST_EVENT_KEY` | `local-dev-event-key` | same | Inngest SDK |
| `INNGEST_SIGNING_KEY` | `local-dev-signing-key` | same | Inngest SDK |
| `KAFKA_CLIENT_ID` | `wms-mvp-local` | same | kafkajs (optional override) |
| `SHOPIFY_SHOP_DOMAIN` | from `.env` | from `.env` | inventory sync, OAuth token script |
| `SHOPIFY_CLIENT_ID` | — (호스트 전용) | from `.env` | `npm run shopify:token` (Partner app OAuth) |
| `SHOPIFY_CLIENT_SECRET` | — (호스트 전용) | from `.env` | `npm run shopify:token` |
| `SHOPIFY_OAUTH_PORT` | — (호스트 전용, default `53682`) | from `.env` | OAuth callback `http://localhost:{PORT}/callback` |
| `SHOPIFY_SCOPES` | — (optional) | `read_locations,read_products,write_inventory` | OAuth token script (override 가능) |
| `SHOPIFY_ADMIN_ACCESS_TOKEN` | from `.env` | from `.env` | inventory sync (Admin API 인증) |
| `SHOPIFY_API_VERSION` | `2026-04` (compose default) | `2026-04` (`.env.example`) | inventory sync GraphQL API 버전 |
| `CRITICAL_STOCK_THRESHOLD` | `0` (default) | `0` | Track 2 즉시 sync 트리거 (`stock <= threshold`) |
| `TEST_SKU` | — | `sku-managed-1` (optional) | `npm run test:inventory-change` 기본 SKU override |
| `TEST_LOCATION` | — | `Shop location` (optional) | `npm run test:inventory-change` 기본 location override |
| `TEST_DELTA` | — | `-5` (optional) | `npm run test:inventory-change` 기본 delta override |

## Shopify Admin API Token 발급

`inventorySync` / `inventoryCriticalSync`는 `SHOPIFY_ADMIN_ACCESS_TOKEN`이 필요합니다. Partner Dashboard에 등록한 앱(Client ID/Secret) 기준으로 호스트에서 OAuth code grant를 실행합니다.

### 사전 준비

1. [Shopify Partner Dashboard](https://partners.shopify.com/) → Apps → 앱 선택
2. **Configuration** → **Allowed redirect URLs**에 callback URL 등록:
   ```
   http://localhost:53682/callback
   ```
   Windows에서 포트 `53682`가 `EACCES`이면 `.env`에 `SHOPIFY_OAUTH_PORT=3456` 설정 후 `http://localhost:3456/callback` 등록
3. `.env`에 아래 값 설정 ([`.env.example`](.env.example) 참고):

```dotenv
SHOPIFY_SHOP_DOMAIN=your-store.myshopify.com
SHOPIFY_CLIENT_ID=your_client_id
SHOPIFY_CLIENT_SECRET=shpss_...
SHOPIFY_OAUTH_PORT=3456
```

### 토큰 발급

```powershell
cp .env.example .env   # 최초 1회
# .env 편집 후:
npm run shopify:token
```

1. 브라우저에서 앱 설치/승인 (터미널 URL을 **전체 복사**해서 열기 — `&` 포함)
2. 승인 완료 시 터미널 출력:
   ```
   SHOPIFY_ADMIN_ACCESS_TOKEN=shpat_...
   # granted scopes: read_locations,read_products,write_inventory
   ```
3. 출력된 `SHOPIFY_ADMIN_ACCESS_TOKEN=` 줄을 `.env`에 붙여넣기
4. Docker 앱 재기동:
   ```powershell
   docker-compose up -d --force-recreate app
   ```

> **스크립트**: [`scripts/get-shopify-token.ts`](scripts/get-shopify-token.ts) — HMAC 검증, code exchange, stdout 출력만 (`.env` 자동 덮어쓰기 없음).
> **Custom app (Admin → Develop apps)**: Admin에서 직접 `shpat_` 토큰을 복사하면 OAuth 스크립트 없이 `.env`에 붙여넣어도 됩니다.

## Project Structure

```
wms-mvp-local/
├── Dockerfile
├── docker-compose.yml
├── package.json
├── tsconfig.json
├── scripts/
│   ├── get-shopify-token.ts           # Shopify OAuth → SHOPIFY_ADMIN_ACCESS_TOKEN 발급
│   ├── publish-inventory-change.ts    # Kafka wms.inventory.changes 테스트 메시지 발행
│   └── verify-inventory-adjust.ts     # inventoryAdjustQuantities API 검증 (개발용)
├── prisma/
│   └── schema.prisma            # WmsOrder, WmsInventory, WmsInventoryChange
├── supabase/
│   └── config.toml              # Supabase CLI local config
├── tests/
│   └── stress-test.ts           # k6 webhook 부하 테스트
└── src/
    ├── index.ts                 # Bootstrap: Prisma → 2×Kafka Consumer → 2×BullMQ → Express
    ├── config/
    │   ├── kafka.ts             # shopify.orders.created + wms.inventory.changes
    │   ├── redis.ts
    │   └── prisma.ts
    ├── api/
    │   └── server.ts            # Express, webhook, Bull-board, Inngest serve
    ├── services/
    │   ├── shopify.ts           # Shopify Admin GraphQL helper
    │   └── shopifyInventoryAdjust.ts  # pushPendingDeltas() — bulk resolve + chunked push (10/chunk)
    └── workers/
        ├── bullmq/
        │   ├── worker.ts        # ETL + prisma.wmsOrder.create (PENDING)
        │   └── inventoryChangeWorker.ts  # delta intake + critical event
        └── inngest/
            ├── client.ts         # Inngest client + orderWorkflow
            ├── inventorySync.ts  # Track 1: cron */15m + inventory/sync.requested
            └── inventoryCriticalSync.ts  # Track 2: inventory/critical.detected
```

## NPM Scripts

| Script | Command | Description |
|--------|---------|-------------|
| `build` | `npm run build` | TypeScript → `dist/` |
| `start` | `npm start` | `node dist/index.js` |
| `dev` | `npm run dev` | `tsx watch src/index.ts` (호스트 dev) |
| `shopify:token` | `npm run shopify:token` | OAuth로 `SHOPIFY_ADMIN_ACCESS_TOKEN` 발급 (호스트 `.env` 필요) |
| `test:inventory-change` | `npm run test:inventory-change` | Kafka `wms.inventory.changes` 테스트 메시지 발행 (기본: `sku-managed-1`, `Shop location`, delta `-5`) |
| `test:stress` | `npm run test:stress` | k6 webhook 부하 테스트 |

## Local Development (without Docker app)

```bash
# 1. Supabase + Redis/Kafka 별도 기동
supabase start

# 2. 앱을 호스트에서 직접 실행
cp .env.example .env
# 호스트에서 Kafka producer/consumer 사용 시:
# KAFKA_BROKER=localhost:9094
npm install
npx prisma db push
npm run dev
```

## Verification Checklist

### Order pipeline

1. `supabase start` → Studio http://localhost:54323 접속 가능
2. `docker-compose up --build` → `[prisma] database connected` → `[app] ready on :3000`
3. POST webhook → `202 { "accepted": true }`
4. Re-POST same webhook → `200 { "duplicated": true }`
5. Bull-board → `wms-etl` job completed
6. Supabase Studio → `WmsOrder` row: `PENDING` → `READY_TO_SHIP` after ~10s
7. Inngest UI → `orderWorkflow` shows `finalize-wms-allocation` succeeded

### Inventory sync (Two-Track, Shopify push)

> **사전 조건**
> 1. `.env`에 `SHOPIFY_SHOP_DOMAIN`, `SHOPIFY_ADMIN_ACCESS_TOKEN` 설정 ([Shopify Admin API Token 발급](#shopify-admin-api-token-발급) 참고)
> 2. `docker-compose up -d --force-recreate app`으로 토큰 반영
> 3. Shopify Admin에 테스트 SKU + 동일 이름 Location 존재, variant **Track quantity** 활성화
> 4. Supabase `WmsInventory`에 해당 SKU 스냅샷 행 존재 (Kafka intake 전제)

#### 0. Shopify Location / SKU 확인

Shopify Admin → **Settings** → **Locations** → Location 이름 확인 (예: `Shop location`)

Shopify Admin → **Products** → variant → **SKU** 및 **Inventory tracked** 확인

#### 1. WmsInventory 스냅샷 시드

**Supabase Studio** (http://localhost:54323) → **SQL Editor**:

```sql
INSERT INTO "WmsInventory" ("sku", "stock", "location", "isSynced", "updatedAt")
VALUES ('sku-managed-1', 100, 'Shop location', true, NOW())
ON CONFLICT ("sku") DO UPDATE
SET "stock" = 100, "location" = 'Shop location', "updatedAt" = NOW();
```

예시 SKU `sku-managed-1` / Location `Shop location` — 아래 테스트 스크립트 기본값과 동일. 다른 SKU를 쓰면 Shopify Admin 값과 맞출 것.

#### 2. Track 1 테스트 — 일반 delta (bulk batch)

Kafka `wms.inventory.changes` 토픽에 일반 변동 메시지 발행 (예: 100 → 95, delta `-5`).

**권장 — 테스트 스크립트** ([`scripts/publish-inventory-change.ts`](scripts/publish-inventory-change.ts)):

호스트 `.env`에 `KAFKA_BROKER=localhost:9094` 설정 후:

```powershell
npm run test:inventory-change
```

기본 payload:

```json
{ "sku": "sku-managed-1", "locationName": "Shop location", "delta": -5 }
```

delta 변경:

```powershell
npm run test:inventory-change -- -3
```

환경변수 override:

```powershell
$env:TEST_SKU="other-sku"; $env:TEST_DELTA="-1"; npm run test:inventory-change
```

**대안 — PowerShell** (`kafka-console-producer`, Kafka 컨테이너 내부 broker):

```powershell
$msg = '{"sku":"sku-managed-1","locationName":"Shop location","delta":-5}'
$msg | docker exec -i wms-mvp-local-kafka-1 kafka-console-producer `
  --bootstrap-server localhost:9092 `
  --topic wms.inventory.changes
```

> 컨테이너 이름은 `docker ps`로 확인. `docker exec` 안에서는 `localhost:9092`; 호스트 Node 스크립트는 `localhost:9094`.

**확인 체크리스트**

| Check | Where | Expected |
|-------|-------|----------|
| Kafka consume | `docker-compose logs app` | `[kafka] dispatched inventory change sku=... delta=-5` |
| BullMQ job | http://localhost:3000/admin/queues → `inventory-change` | job `completed` |
| Delta row | Studio → `wms_inventory_changes` | `deltaQty=-5`, `isShopifyPushed=false` |
| Snapshot | Studio → `WmsInventory` | `stock=95` (100에서 -5) |
| Critical event | Inngest UI | **발행 안 됨** (`stock > 0`) |

**Track 1 push 트리거** (수동, 권장):

Inngest UI (http://localhost:8288) → **Functions** → `Bulk inventory sync to Shopify` → **Invoke**
- Event: `inventory/sync.requested`, Data: `{}`

```powershell
Invoke-RestMethod `
  -Uri http://localhost:8288/e/local-dev-event-key `
  -Method POST `
  -Body '{"name":"inventory/sync.requested","data":{}}' `
  -ContentType "application/json"
```

**Inngest Run 결과**

| Step | Expected |
|------|----------|
| `push-pending-deltas` | `{ pushed: 1, attempted: 1, failed: 0, failedReasons: [], skipped: false }` |
| **Return** | 동일 (partial failure 시 `failed > 0`, `failedReasons`에 청크별 상세) |

Studio → `wms_inventory_changes`: `isShopifyPushed = true`

Shopify Admin → variant **Inventory** → 해당 Location **On hand**가 `changeFromQuantity + delta`로 설정됨 (예: 50 + 2 = 52).

**Cron 대기**: 최대 15분 (`*/15 * * * *`)

#### 3. Track 2 테스트 — critical stock (즉시 sync)

스냅샷을 품절 임박 수준으로 맞춘 뒤, 품절을 유발하는 delta 발행:

```sql
-- Studio SQL Editor: stock을 5로 설정
UPDATE "WmsInventory" SET "stock" = 5, "updatedAt" = NOW() WHERE sku = 'sku-managed-1';
```

```powershell
npm run test:inventory-change -- -5
```

또는 `kafka-console-producer`:

```powershell
$msg = '{"sku":"sku-managed-1","locationName":"Shop location","delta":-5}'
$msg | docker exec -i wms-mvp-local-kafka-1 kafka-console-producer `
  --bootstrap-server localhost:9092 `
  --topic wms.inventory.changes
```

**확인 체크리스트**

| Check | Where | Expected |
|-------|-------|----------|
| BullMQ log | `docker-compose logs app` | `critical=true` |
| Inngest Run | http://localhost:8288 → `Instant inventory sync (critical)` | `push-critical` step 성공 |
| Delta pushed | Studio → `wms_inventory_changes` | `isShopifyPushed = true` |
| Shopify | Admin Inventory | On hand가 delta만큼 반영 |

> Track 2는 **배치 대기 없이** 해당 SKU의 미처리 delta를 즉시 drain합니다. Track 1 cron을 기다릴 필요 없습니다.

#### 4. Supabase Studio DB 확인

**SQL Editor**:

```sql
-- 스냅샷
SELECT sku, stock, location, "updatedAt"
FROM "WmsInventory"
WHERE sku = 'sku-managed-1';

-- delta 큐
SELECT id, sku, "locationName", "deltaQty", "isShopifyPushed", "createdAt"
FROM "wms_inventory_changes"
WHERE sku = 'sku-managed-1'
ORDER BY id DESC;
```

#### 5. Idempotency (skip 확인)

모든 delta가 `isShopifyPushed = true`인 상태에서 `inventory/sync.requested` 재발행:

```json
{ "skipped": true, "reason": "no-pending-deltas", "pushed": 0, "attempted": 0 }
```

#### 6. `CRITICAL_STOCK_THRESHOLD` 조정 (선택)

`.env`에 `CRITICAL_STOCK_THRESHOLD=10` 설정 시 `stock <= 10`에서 Track 2가 발동합니다.

```powershell
docker-compose up -d --force-recreate app
```

#### 실패 시나리오

| 증상 | 원인 | 조치 |
|------|------|------|
| `SHOPIFY_ADMIN_ACCESS_TOKEN is required` | 토큰 미설정 / 컨테이너 미반영 | `.env` 설정 후 `docker-compose up -d --force-recreate app` |
| `Shopify 401` | 토큰 오류 / 스코프 부족 | `read_locations`, `read_products`, `write_inventory` scope 확인 |
| `{ skipped: true, reason: "no-resolvable-deltas" }` | SKU 또는 location 이름 불일치 | Shopify Location 이름·SKU 정확히 매칭 (`locationName` 필드) |
| `Shopify userErrors` | Track quantity 미활성 / API 버전 불일치 | variant inventory tracking 활성화; `SHOPIFY_API_VERSION=2026-04` 확인 |
| `Shopify 429` / rate limit | 대량 mutation 동시 호출 (구 코드) | chunked push (`MUTATION_CHUNK_SIZE=10` 순차); `docker compose build --no-cache app` |
| `INVALID_VARIABLE` (`ledgerLines`, missing `name`) | 구 mutation payload / stale Docker image | `docker compose build --no-cache app` 후 재기동 |
| `changeFromQuantity` required / `@idempotent` required | 2026-04 필수 필드 누락 | 최신 `shopifyInventoryAdjust.ts` 사용 여부 확인 |
| `INVALID_QUANTITY_NAME` (`on_hand` on adjust) | `inventoryAdjustQuantities`로 on_hand 시도 | `inventorySetQuantities` 사용 (현재 코드 기본) |
| `CHANGE_FROM_QUANTITY_STALE` | push 직전 Shopify 재고 변동 (주문 끼어들기 등) | 해당 청크만 `failedReasons` 기록, 나머지 청크는 push 완료. 실패 row는 다음 cron에서 재시도 |
| `{ failed: N, failedReasons: [...] }` in Inngest result | 일부 청크 push 실패 (의도된 partial failure) | Inngest UI에서 `failedReasons` 확인. Studio에서 실패 row `isShopifyPushed=false` 확인 후 다음 sync 대기 |
| BullMQ inventory job failed | `WmsInventory`에 SKU 없음 | Studio에서 스냅샷 행 먼저 INSERT |
| Track 2 미발동 | threshold 미달 | `CRITICAL_STOCK_THRESHOLD` 및 현재 `stock` 확인 |
| Kafka 메시지 무응답 | topic/consumer 미기동 | `docker-compose logs app`에서 `inventory change consumer started` 확인 |
| `test:inventory-change` connection refused | 호스트에서 `KAFKA_BROKER=kafka:9092` 사용 | 호스트 스크립트는 `KAFKA_BROKER=localhost:9094` (Docker `app`은 compose가 `kafka:9092` 유지) |

### Stress test (k6)

Webhook 멱등성·처리량 검증 ([`tests/stress-test.ts`](tests/stress-test.ts)):

```powershell
# k6 설치 필요. 미설치 시 Docker:
docker run --rm -i `
  -v "${PWD}/tests:/tests" `
  -e TARGET_URL=http://host.docker.internal:3000 `
  grafana/k6 run /tests/stress-test.js
```

호스트에 k6가 있으면:

```powershell
npm run test:stress
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `P1001: Can't reach database server at host.docker.internal:54322` | Supabase CLI not running | Run `supabase start` before `docker-compose up` |
| Orphan `supabase-db` container warning | Legacy compose service removed | `docker-compose down --remove-orphans` |
| `WmsOrder` table missing in Studio | Schema not pushed yet | Restart `app` container (runs `prisma db push` on start) |
| OAuth `redirect_uri is missing` | Windows `cmd start`가 URL을 `&`에서 잘림 | 터미널 URL **전체** 복사·붙여넣기 (스크립트는 `rundll32` 사용으로 수정됨) |
| OAuth `listen EACCES :53682` | Windows 예약 포트 | `.env`에 `SHOPIFY_OAUTH_PORT=3456`, Partner app redirect URL도 동일 포트로 등록 |
| Inventory sync env 변경 미반영 | Docker는 기동 시 env 고정 | `docker-compose up -d --force-recreate app` |
| Inventory sync 코드 변경 미반영 | Docker image에 stale `dist/` | `docker compose build --no-cache app && docker compose up -d app` |
| `Shopify 429` during inventory sync | 대량 mutation 단일 호출 (구 코드) | chunked push (`MUTATION_CHUNK_SIZE=10`) 반영 후 image 재빌드 |
| Inngest shows `failed > 0` but step succeeded | partial failure by design | `failedReasons` 확인; `isShopifyPushed=false` row는 다음 cron 재시도 |
| `wms_inventory_changes` table missing | Schema not pushed | Restart `app` container (`prisma db push` on start) |
| Inventory Kafka message ignored | Consumer not started | `docker-compose logs app` → `inventory change consumer started` |
| `test:inventory-change` ECONNREFUSED / `kafka:9092` | 호스트에서 Docker 내부 broker 주소 사용 | `.env`에 `KAFKA_BROKER=localhost:9094` (compose `app`은 `kafka:9092`로 override됨) |
