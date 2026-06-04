# pg-listener

PostgreSQL [`LISTEN`](https://www.postgresql.org/docs/current/sql-listen.html) / [`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) for Tokio: one `PgListener` per pool, ref-counted subscriptions, typed JSON channels, RAII `UNLISTEN`.

## Install

```bash
cargo add pg-listener
```

## Usage

```rust
use std::time::Duration;

use pg_listener::{ListenerService, NotificationListener, TypedChannel};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio_graceful_shutdown::Toplevel;

#[derive(Serialize, Deserialize)]
struct OrderUpdated { order_id: u64 }

let pool = PgPool::connect(&database_url).await?;
let service = ListenerService::try_new(&pool).await?;
let listener: NotificationListener = service.notification_listener();

tokio::spawn(async move {
    Toplevel::new(async |subsys| {
        service.start(subsys);
    })
    .handle_shutdown_requests(Duration::from_secs(1))
    .await
    .ok();
});

let channel = TypedChannel::<OrderUpdated>::try_from("orders".to_owned())?;
let mut guard = listener.listen_typed(channel).await?;

TypedChannel::<OrderUpdated>::try_from("orders".to_owned())?
    .publish_pool(&pool, &OrderUpdated { order_id: 42 })
    .await?;
let n = guard.recv().await?;
```

Channel names must be 1–63 characters (`Channel::try_from` / `TypedChannel::try_from`).

## Development

```bash
task test
task test:e2e
```
