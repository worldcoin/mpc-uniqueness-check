use std::marker::PhantomData;

use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::{Postgres, QueryBuilder};

use self::kinds::DbKind;
use crate::config::DbConfig;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations/");

pub mod impls;
pub mod kinds;

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

        let pool = sqlx::Pool::connect(&config.url).await?;

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
        let items: Vec<(i64, K::Item)> = sqlx::query_as(&format!(
            r#"
                SELECT id, {column}
                FROM {table}
                WHERE id > $1
                ORDER BY id ASC
            "#,
            column = K::COLUMN_NAME,
            table = K::TABLE_NAME,
        ))
        .bind(id as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(filter_sequential_items(items, 1 + id as i64))
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

fn filter_sequential_items<T>(
    items: impl IntoIterator<Item = (i64, T)>,
    first_id: i64,
) -> Vec<T> {
    let mut last_key = None;

    let mut items = items.into_iter();

    std::iter::from_fn(move || {
        let (key, value) = items.next()?;

        if let Some(last_key) = last_key {
            if key != last_key + 1 {
                return None;
            }
        } else if key != first_id {
            return None;
        }

        last_key = Some(key);

        Some(value)
    })
    .fuse()
    .collect()
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
}
