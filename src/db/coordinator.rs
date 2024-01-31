use sqlx::migrate::Migrator;
use sqlx::{Postgres, QueryBuilder};

use crate::bits::Bits;

static MIGRATOR: Migrator = sqlx::migrate!("migrations/coordinator/");

pub struct CoordinatorDb {
    pool: sqlx::Pool<Postgres>,
}

impl CoordinatorDb {
    pub async fn new(url: &str) -> eyre::Result<Self> {
        let pool = sqlx::Pool::connect(url).await?;

        MIGRATOR.run(&pool).await?;

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

    #[tokio::test]
    async fn fetch_on_empty() -> eyre::Result<()> {
        let db = docker_db::Postgres::spawn().await?;
        let url = format!("postgres://postgres:postgres@{}", db.socket_addr());

        let db = CoordinatorDb::new(&url).await?;

        let masks = db.fetch_masks(0).await?;

        assert!(masks.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn insert_and_fetch() -> eyre::Result<()> {
        let db = docker_db::Postgres::spawn().await?;
        let url = format!("postgres://postgres:postgres@{}", db.socket_addr());

        let db = CoordinatorDb::new(&url).await?;

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
        let db = docker_db::Postgres::spawn().await?;
        let url = format!("postgres://postgres:postgres@{}", db.socket_addr());

        let db = CoordinatorDb::new(&url).await?;

        let mut rng = thread_rng();

        let masks = vec![(0, rng.gen::<Bits>()), (1, rng.gen::<Bits>())];

        db.insert_masks(&masks).await?;

        let fetched_masks = db.fetch_masks(1).await?;

        assert_eq!(fetched_masks[0], masks[1].1);

        Ok(())
    }
}
