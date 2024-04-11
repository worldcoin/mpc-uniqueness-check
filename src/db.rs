pub mod impls;

use futures::{Stream, TryStreamExt};
use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::{Postgres, QueryBuilder};
use sysinfo::System;

use crate::bits::Bits;
use crate::config::DbConfig;
use crate::distance::EncodedBits;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/");

pub struct Db {
    pool: sqlx::Pool<Postgres>,
}

impl Db {
    pub async fn new(config: &DbConfig) -> eyre::Result<Self> {
        tracing::info!("Connecting to database");

        if config.create
            && !sqlx::Postgres::database_exists(&config.url).await?
        {
            tracing::info!("Creating database");
            sqlx::Postgres::create_database(&config.url).await?;
        }

        let pool = sqlx::Pool::connect(&config.url).await?;

        if config.migrate {
            tracing::info!("Running migrations");
            MIGRATOR.run(&pool).await?;
        }

        tracing::info!("Connected to database");

        Ok(Self { pool })
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch_masks(&self, id: usize) -> eyre::Result<Vec<Bits>> {
        let masks_stream = sqlx::query_as(
            r#"
            SELECT id, mask
            FROM masks
            WHERE id > $1
            ORDER BY id ASC
            "#,
        )
        .bind(id as i64)
        .fetch(&self.pool);

        log_mem_usage();

        Ok(stream_sequential_items(masks_stream, 1 + id as i64).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch_latest_mask_id(&self) -> eyre::Result<u64> {
        let mask_id = sqlx::query_as::<_, (i64,)>(
            r#"
            SELECT id
            FROM masks
            ORDER BY id DESC
            LIMIT 1
        "#,
        )
        .fetch_one(&self.pool)
        .await;

        match mask_id {
            Ok(mask_id) => Ok(mask_id.0 as u64),
            Err(sqlx::Error::RowNotFound) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn insert_masks(
        &self,
        masks: &[(u64, Bits)],
    ) -> eyre::Result<()> {
        let mut builder =
            QueryBuilder::new("INSERT INTO masks (id, mask) VALUES ");

        for (idx, (id, mask)) in masks.iter().enumerate() {
            if idx > 0 {
                builder.push(", ");
            }
            builder.push("(");
            builder.push_bind(*id as i64);
            builder.push(", ");
            builder.push_bind(mask);
            builder.push(")");
        }

        builder.push(" ON CONFLICT (id) DO NOTHING");

        let query = builder.build();

        query.execute(&self.pool).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch_shares(
        &self,
        id: usize,
    ) -> eyre::Result<Vec<EncodedBits>> {
        let shares_stream = sqlx::query_as(
            r#"
            SELECT id, share
            FROM shares
            WHERE id > $1
            ORDER BY id ASC
            "#,
        )
        .bind(id as i64)
        .fetch(&self.pool);

        log_mem_usage();

        Ok(stream_sequential_items(shares_stream, 1 + id as i64).await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch_latest_share_id(&self) -> eyre::Result<u64> {
        let share_id = sqlx::query_as::<_, (i64,)>(
            r#"
            SELECT id
            FROM shares
            ORDER BY id DESC
            LIMIT 1
        "#,
        )
        .fetch_one(&self.pool)
        .await;

        match share_id {
            Ok(share_id) => Ok(share_id.0 as u64),
            Err(sqlx::Error::RowNotFound) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn insert_shares(
        &self,
        shares: &[(u64, EncodedBits)],
    ) -> eyre::Result<()> {
        let mut builder =
            QueryBuilder::new("INSERT INTO shares (id, share) VALUES ");

        for (idx, (id, share)) in shares.iter().enumerate() {
            if idx > 0 {
                builder.push(", ");
            }
            builder.push("(");
            builder.push_bind(*id as i64);
            builder.push(", ");
            builder.push_bind(share);
            builder.push(")");
        }

        builder.push(" ON CONFLICT (id) DO NOTHING");

        let query = builder.build();

        query.execute(&self.pool).await?;

        Ok(())
    }

    /// Removes masks and shares with serial IDs larger than `serial_id`.
    #[tracing::instrument(skip(self))]
    pub async fn prune_items(&self, serial_id: u64) -> eyre::Result<()> {
        sqlx::query(
            r#"
            DELETE FROM masks
            WHERE id > $1
        "#,
        )
        .bind(serial_id as i64)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM shares
            WHERE id > $1
        "#,
        )
        .bind(serial_id as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

async fn stream_sequential_items<T, E>(
    mut stream: impl Stream<Item = Result<(i64, T), E>> + Unpin,
    first_id: i64,
) -> Result<Vec<T>, E> {
    let mut items = vec![];

    let mut next_key = first_id;
    let mut counter = 0;
    while let Some((key, value)) = stream.try_next().await? {
        if key != next_key {
            break;
        }

        next_key = key + 1;
        items.push(value);

        if counter % 1000 == 0 {
            log_mem_usage();
        }
        counter += 1;
    }

    Ok(items)
}

fn log_mem_usage() {
    let mut sys = System::new_all();

    sys.refresh_all();

    let total_memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let total_swap = sys.total_swap();
    let used_swap = sys.used_swap();

    tracing::info!(
        total_memory,
        used_memory,
        total_swap,
        used_swap,
        "Memory usage"
    );
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};
    use testcontainers::{clients, Container};

    use super::*;

    async fn setup(
        docker: &testcontainers::clients::Cli,
    ) -> eyre::Result<(Db, Container<testcontainers_modules::postgres::Postgres>)>
    {
        let postgres_container = docker.run(
            testcontainers_modules::postgres::Postgres::default()
                .with_host_auth(),
        );

        let mapped_port = postgres_container.get_host_port_ipv4(5432);

        let url = format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            mapped_port
        );
        let db = Db::new(&DbConfig {
            url,
            migrate: true,
            create: true,
        })
        .await?;

        Ok((db, postgres_container))
    }

    #[tokio::test]
    async fn fetch_on_empty() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let masks = db.fetch_masks(0).await?;

        assert!(masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_fetch() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![(1, rng.gen::<Bits>()), (2, rng.gen::<Bits>())];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(0).await?;
        let masks_without_ids =
            masks.iter().map(|(_, mask)| *mask).collect::<Vec<_>>();

        assert_eq!(fetched_masks.len(), 2);
        assert_eq!(fetched_masks, masks_without_ids);

        Ok(())
    }

    #[tokio::test]
    async fn partial_fetch() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![(1, rng.gen::<Bits>()), (2, rng.gen::<Bits>())];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(1).await?;

        assert_eq!(fetched_masks[0], masks[1].1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_shares_on_empty() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let shares = db.fetch_shares(0).await?;

        assert!(shares.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_fetch_shares() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let shares =
            vec![(1, rng.gen::<EncodedBits>()), (2, rng.gen::<EncodedBits>())];

        db.insert_shares(&shares).await?;

        let fetched_shares = db.fetch_shares(0).await?;
        let shares_without_ids =
            shares.iter().map(|(_, share)| *share).collect::<Vec<_>>();

        assert_eq!(fetched_shares.len(), 2);
        assert_eq!(fetched_shares, shares_without_ids);

        Ok(())
    }

    #[tokio::test]
    async fn partial_fetch_shares() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let shares =
            vec![(1, rng.gen::<EncodedBits>()), (2, rng.gen::<EncodedBits>())];

        db.insert_shares(&shares).await?;

        let fetched_shares = db.fetch_shares(1).await?;

        assert_eq!(fetched_shares[0], shares[1].1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_latest_mask_id() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (5, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
            (8, rng.gen::<Bits>()),
        ];

        db.insert_masks(&masks).await?;

        let latest_mask_id = db.fetch_latest_mask_id().await?;

        assert_eq!(latest_mask_id, 8);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_latest_share_id() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let shares = vec![
            (1, rng.gen::<EncodedBits>()),
            (2, rng.gen::<EncodedBits>()),
            (5, rng.gen::<EncodedBits>()),
            (6, rng.gen::<EncodedBits>()),
            (8, rng.gen::<EncodedBits>()),
        ];

        db.insert_shares(&shares).await?;

        let latest_share_id = db.fetch_latest_share_id().await?;

        assert_eq!(latest_share_id, 8);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_shares_returns_sequential_data() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let shares = vec![
            (1, rng.gen::<EncodedBits>()),
            (2, rng.gen::<EncodedBits>()),
            (5, rng.gen::<EncodedBits>()),
            (6, rng.gen::<EncodedBits>()),
            (8, rng.gen::<EncodedBits>()),
        ];

        db.insert_shares(&shares).await?;

        let fetched_shares = db.fetch_shares(0).await?;

        assert_eq!(fetched_shares.len(), 2);
        assert_eq!(fetched_shares[0], shares[0].1);
        assert_eq!(fetched_shares[1], shares[1].1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_masks_returns_sequential_data() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(1).await?;

        assert_eq!(fetched_masks.len(), 3);
        assert_eq!(fetched_masks[0], masks[1].1);
        assert_eq!(fetched_masks[1], masks[2].1);
        assert_eq!(fetched_masks[2], masks[3].1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_masks_returns_nothing_if_non_sequential() -> eyre::Result<()>
    {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(4).await?;

        assert!(fetched_masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn fetch_masks_returns_nothing_if_missing_first() -> eyre::Result<()>
    {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (5, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(0).await?;

        assert!(fetched_masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn prune_items() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (5, rng.gen::<Bits>()),
        ];

        db.insert_masks(&masks).await?;

        let shares = vec![
            (1, rng.gen::<EncodedBits>()),
            (2, rng.gen::<EncodedBits>()),
            (3, rng.gen::<EncodedBits>()),
            (4, rng.gen::<EncodedBits>()),
            (5, rng.gen::<EncodedBits>()),
        ];

        db.insert_shares(&shares).await?;

        let fetched_masks = db.fetch_masks(0).await?;
        let fetched_shares = db.fetch_shares(0).await?;

        assert_eq!(fetched_masks.len(), 5);
        assert_eq!(fetched_shares.len(), 5);

        db.prune_items(2).await?;

        let fetched_masks = db.fetch_masks(0).await?;
        let fetched_shares = db.fetch_shares(0).await?;

        assert_eq!(fetched_masks.len(), 2);
        assert_eq!(fetched_shares.len(), 2);

        Ok(())
    }
}
