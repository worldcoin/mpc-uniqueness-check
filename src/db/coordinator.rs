use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::{Postgres, QueryBuilder};

use crate::bits::Bits;
use crate::config::DbConfig;

static MIGRATOR: Migrator = sqlx::migrate!("migrations/coordinator/");

pub struct CoordinatorDb {
    pool: sqlx::Pool<Postgres>,
}

impl CoordinatorDb {
    pub async fn new(config: &DbConfig) -> eyre::Result<Self> {
        if config.create && !sqlx::Postgres::database_exists(&config.url).await? {
            sqlx::Postgres::create_database(&config.url).await?;
        }

        let pool = sqlx::Pool::connect(&config.url).await?;

        if config.migrate {
            MIGRATOR.run(&pool).await?;
        }

        Ok(Self { pool })
    }

    pub async fn fetch_masks(&self, id: usize) -> eyre::Result<Vec<Bits>> {
        let masks: Vec<(Bits,)> = sqlx::query_as(
            r#"
            SELECT mask
            FROM masks
            WHERE id >= $1
            ORDER BY id ASC
        "#,
        )
        .bind(id as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(masks.into_iter().map(|(mask,)| mask).collect())
    }

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
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};

    use super::*;

    async fn setup() -> eyre::Result<(CoordinatorDb, docker_db::Postgres)> {
        let pg_db = docker_db::Postgres::spawn().await?;
        let url =
            format!("postgres://postgres:postgres@{}", pg_db.socket_addr());

        let db = CoordinatorDb::new(&DbConfig { url, migrate: true, create: true }).await?;

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

        let masks = vec![(0, rng.gen::<Bits>()), (1, rng.gen::<Bits>())];

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

        let masks = vec![(0, rng.gen::<Bits>()), (1, rng.gen::<Bits>())];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(1).await?;

        assert_eq!(fetched_masks[0], masks[1].1);

        Ok(())
    }
}
