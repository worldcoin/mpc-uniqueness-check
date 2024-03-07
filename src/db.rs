pub mod impls;

use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::{Postgres, QueryBuilder};

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
        let masks: Vec<(i64, Bits)> = sqlx::query_as(
            r#"
            SELECT id, mask
            FROM masks
            WHERE id > $1
            ORDER BY id ASC
        "#,
        )
        .bind(id as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(filter_sequential_items(masks, 1 + id as i64))
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
        let shares: Vec<(i64, EncodedBits)> = sqlx::query_as(
            r#"
            SELECT id, share
            FROM shares
            WHERE id > $1
            ORDER BY id ASC
        "#,
        )
        .bind(id as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(filter_sequential_items(shares, 1 + id as i64))
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

    use super::*;

    async fn setup() -> eyre::Result<(Db, docker_db::Postgres)> {
        let pg_db = docker_db::Postgres::spawn().await?;
        let url =
            format!("postgres://postgres:postgres@{}", pg_db.socket_addr());

        let db = Db::new(&DbConfig {
            url,
            migrate: true,
            create: true,
        })
        .await?;

        Ok((db, pg_db))
    }

    #[tokio::test]
    async fn fetch_on_empty() -> eyre::Result<()> {
        let (db, _pg) = setup().await?;

        let masks = db.fetch_masks(0).await?;

        assert!(masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_fetch() -> eyre::Result<()> {
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

        let mut rng = thread_rng();

        let masks = vec![(1, rng.gen::<Bits>()), (2, rng.gen::<Bits>())];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(1).await?;

        assert_eq!(fetched_masks[0], masks[1].1);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_shares_on_empty() -> eyre::Result<()> {
        let (db, _pg) = setup().await?;

        let shares = db.fetch_shares(0).await?;

        assert!(shares.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_fetch_shares() -> eyre::Result<()> {
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

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
        let (db, _pg) = setup().await?;

        let mut rng = thread_rng();

        let masks = vec![
            (1, rng.gen::<Bits>()),
            (2, rng.gen::<Bits>()),
            (3, rng.gen::<Bits>()),
            (4, rng.gen::<Bits>()),
            (6, rng.gen::<Bits>()),
        ];

        db.insert_masks(&masks).await?;

        let shares = vec![
            (1, rng.gen::<EncodedBits>()),
            (2, rng.gen::<EncodedBits>()),
            (3, rng.gen::<EncodedBits>()),
            (4, rng.gen::<EncodedBits>()),
            (6, rng.gen::<EncodedBits>()),
        ];

        db.insert_shares(&shares).await?;

        db.prune_items(2).await?;

        let fetched_masks = db.fetch_masks(0).await?;
        let fetched_shares = db.fetch_shares(0).await?;

        assert_eq!(fetched_masks.len(), 2);
        assert_eq!(fetched_shares.len(), 2);

        Ok(())
    }
}
