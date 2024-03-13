use std::marker::PhantomData;

use futures::stream::FuturesOrdered;
use futures::StreamExt;
use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::{Postgres, QueryBuilder};

use self::kinds::DbKind;
use crate::config::DbConfig;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/");

pub mod impls;
pub mod kinds;

pub const MULTI_CONNECTION_FETCH_THRESHOLD: usize = 10000;

pub struct Db<K> {
    pool: sqlx::Pool<Postgres>,
    _kind: PhantomData<K>,
}

impl<K> Db<K> {
    pub fn switch<T>(self) -> Db<T>
    where
        T: DbKind,
    {
        Db {
            pool: self.pool,
            _kind: PhantomData,
        }
    }
}

impl<K> Db<K>
where
    K: DbKind,
{
    pub async fn new(config: &DbConfig) -> eyre::Result<Self> {
        tracing::info!("Connecting to database");

        if config.create
            && !sqlx::Postgres::database_exists(&config.url).await?
        {
            tracing::info!("Creating database");
            sqlx::Postgres::create_database(&config.url).await?;
        }

        let pool = sqlx::pool::PoolOptions::new()
            .max_connections(1000)
            .connect(&config.url)
            .await?;

        if config.migrate {
            tracing::info!("Running migrations");
            MIGRATOR.run(&pool).await?;
        }

        tracing::info!("Connected to database");

        Ok(Self {
            pool,
            _kind: PhantomData,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch_items(&self, id: usize) -> eyre::Result<Vec<K::Item>> {
        let first_gap = self.find_first_gap(id).await?;
        let num_items = first_gap - id as i64;

        if num_items as usize > MULTI_CONNECTION_FETCH_THRESHOLD {
            self.fetch_items_in_bulk(id, first_gap).await
        } else {
            self.fetch_items_single_query(id, first_gap).await
        }
    }

    async fn find_first_gap(&self, id: usize) -> eyre::Result<i64> {
        let query = format!(
            "WITH RECURSIVE seq AS (
                SELECT $1 + 1 AS val
                UNION ALL
                SELECT val + 1 FROM seq JOIN {table} ON {table}.id = seq.val WHERE val < (SELECT MAX(id) FROM {table})
            ), min_gap AS (
                SELECT MIN(val) AS first_gap FROM seq WHERE val NOT IN (SELECT id FROM {table})
            )
            SELECT COALESCE((SELECT first_gap FROM min_gap), (SELECT MAX(id) + 1 FROM {table}), $1 + 1) AS id_value",
            table = K::TABLE_NAME
        );
        let (first_gap,): (i64,) = sqlx::query_as(&query)
            .bind(id as i64)
            .fetch_one(&self.pool)
            .await?;
        Ok(first_gap)
    }

    async fn fetch_items_in_bulk(
        &self,
        id: usize,
        first_gap: i64,
    ) -> eyre::Result<Vec<K::Item>> {
        let mut fetches = FuturesOrdered::new();

        for start in
            (id..first_gap as usize).step_by(MULTI_CONNECTION_FETCH_THRESHOLD)
        {
            let end = (start + MULTI_CONNECTION_FETCH_THRESHOLD)
                .min(first_gap as usize);
            fetches.push_back(async move {
                let query_template = self.construct_fetch_query();

                tracing::info!("Fetching items {} to {}", start, end);
                let items = self.fetch_range(&query_template, start, end).await;

                tracing::info!("Fetched items {} to {}", start, end);
                items
            });
        }

        let mut all_items = Vec::new();
        while let Some(items) = fetches.next().await {
            all_items.extend(items?);
        }

        tracing::info!("Collected all items");

        Ok(all_items.into_iter().map(|(_id, item)| item).collect())
    }

    async fn fetch_items_single_query(
        &self,
        id: usize,
        first_gap: i64,
    ) -> eyre::Result<Vec<K::Item>> {
        let query = self.construct_fetch_query();

        let items = self.fetch_range(&query, id, first_gap as usize).await?;

        Ok(items.into_iter().map(|(_id, item)| item).collect())
    }

    fn construct_fetch_query(&self) -> String {
        format!(
            "SELECT id, {column} FROM {table} WHERE id > $1 AND id <= $2 ORDER BY id ASC",
            column = K::COLUMN_NAME,
            table = K::TABLE_NAME,
        )
    }

    async fn fetch_range(
        &self,
        query: &str,
        start: usize,
        end: usize,
    ) -> eyre::Result<Vec<(i64, K::Item)>> {
        Ok(sqlx::query_as::<_, (i64, K::Item)>(query)
            .bind(start as i64)
            .bind(end as i64)
            .fetch_all(&self.pool)
            .await?)
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch_latest_item_id(&self) -> eyre::Result<u64> {
        let item_id = sqlx::query_as::<_, (i64,)>(&format!(
            r#"
                SELECT id
                FROM {table}
                ORDER BY id DESC
                LIMIT 1
            "#,
            table = K::TABLE_NAME
        ))
        .fetch_one(&self.pool)
        .await;

        match item_id {
            Ok(item_id) => Ok(item_id.0 as u64),
            Err(sqlx::Error::RowNotFound) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn insert_items(
        &self,
        items: &[(u64, K::Item)],
    ) -> eyre::Result<()> {
        let mut builder = QueryBuilder::new(format!(
            "INSERT INTO {table} (id, {column}) VALUES ",
            table = K::TABLE_NAME,
            column = K::COLUMN_NAME,
        ));

        for (idx, (id, item)) in items.iter().enumerate() {
            if idx > 0 {
                builder.push(", ");
            }
            builder.push("(");
            builder.push_bind(*id as i64);
            builder.push(", ");
            builder.push_bind(item);
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
        sqlx::query(&format!(
            r#"
            DELETE FROM {table}
            WHERE id > $1
            "#,
            table = K::TABLE_NAME
        ))
        .bind(serial_id as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};
    use testcontainers::{clients, Container};

    use super::kinds::Masks;
    use super::*;
    use crate::bits::Bits;

    async fn setup<K>(
        docker: &testcontainers::clients::Cli,
    ) -> eyre::Result<(
        Db<K>,
        Container<testcontainers_modules::postgres::Postgres>,
    )>
    where
        K: DbKind,
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
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let masks = db.fetch_items(0).await?;

        assert!(masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_fetch() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![(1, rng.gen::<Bits>()), (2, rng.gen::<Bits>())];

        db.insert_items(&masks).await?;

        let fetched_masks = db.fetch_items(0).await?;
        let masks_without_ids =
            masks.iter().map(|(_, mask)| *mask).collect::<Vec<_>>();

        assert_eq!(fetched_masks.len(), 2);
        assert_eq!(fetched_masks, masks_without_ids);

        Ok(())
    }

    #[tokio::test]
    async fn partial_fetch() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![(1, rng.gen::<Bits>()), (2, rng.gen::<Bits>())];

        db.insert_items(&masks).await?;

        let fetched_masks = db.fetch_items(1).await?;

        assert_eq!(fetched_masks[0], masks[1].1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_masks_returns_sequential_data() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_items(&masks).await?;

        let fetched_masks = db.fetch_items(1).await?;

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
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_items(&masks).await?;

        let fetched_masks = db.fetch_items(4).await?;

        assert!(fetched_masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn fetch_masks_returns_nothing_if_missing_first() -> eyre::Result<()>
    {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (5, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_items(&masks).await?;

        let fetched_masks = db.fetch_items(0).await?;

        assert!(fetched_masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn prune_items() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (5, rng.gen::<Bits>()),
        ];

        db.insert_items(&masks).await?;

        let fetched_masks = db.fetch_items(0).await?;

        assert_eq!(fetched_masks.len(), 5);

        db.prune_items(2).await?;

        let fetched_masks = db.fetch_items(0).await?;

        assert_eq!(fetched_masks.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn large_fetch() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        let mut rng = thread_rng();

        let masks = (1..3000)
            .map(|id| (id, rng.gen::<Bits>()))
            .collect::<Vec<_>>();

        assert!(masks.len() > MULTI_CONNECTION_FETCH_THRESHOLD);

        db.insert_items(&masks).await?;

        let expected_masks =
            masks.iter().map(|(_, mask)| *mask).collect::<Vec<_>>();
        let fetched_masks = db.fetch_items(0).await?;

        assert_eq!(fetched_masks, expected_masks);

        Ok(())
    }

    #[tokio::test]
    async fn large_fetch_with_gap() -> eyre::Result<()> {
        let docker = clients::Cli::default();
        let (db, _pg) = setup::<Masks>(&docker).await?;

        const GAP: usize = 1534;

        let mut rng = thread_rng();

        let mut masks = (1..3000)
            .map(|id| (id, rng.gen::<Bits>()))
            .collect::<Vec<_>>();

        masks.remove(GAP);

        assert!(masks.len() > MULTI_CONNECTION_FETCH_THRESHOLD);

        db.insert_items(&masks).await?;

        let expected_masks = masks[..GAP]
            .iter()
            .map(|(_, mask)| *mask)
            .collect::<Vec<_>>();
        let fetched_masks = db.fetch_items(0).await?;

        assert_eq!(fetched_masks.len(), expected_masks.len());
        assert_eq!(fetched_masks, expected_masks);

        Ok(())
    }
}
