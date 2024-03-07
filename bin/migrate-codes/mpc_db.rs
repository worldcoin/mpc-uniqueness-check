use eyre::ContextCompat;
use mpc::config::DbConfig;
use mpc::db::Db;

pub struct MPCDb {
    pub left_coordinator_db: Db,
    pub left_participant_dbs: Vec<Db>,
    pub right_coordinator_db: Db,
    pub right_participant_dbs: Vec<Db>,
}

impl MPCDb {
    pub async fn new(
        left_coordinator_db_url: String,
        left_participant_db_urls: Vec<String>,
        right_coordinator_db_url: String,
        right_participant_db_urls: Vec<String>,
        no_migrate_or_create: bool,
    ) -> eyre::Result<Self> {
        let migrate = !no_migrate_or_create;
        let create = !no_migrate_or_create;

        tracing::info!("Connecting to left coordinator db");
        let left_coordinator_db = Db::new(&DbConfig {
            url: left_coordinator_db_url,
            migrate,
            create,
        })
        .await?;

        let mut left_participant_dbs = vec![];

        for (i, url) in left_participant_db_urls.into_iter().enumerate() {
            tracing::info!(participant=?i, "Connecting to left participant db");
            let db = Db::new(&DbConfig {
                url,
                migrate,
                create,
            })
            .await?;
            left_participant_dbs.push(db);
        }

        tracing::info!("Connecting to right coordinator db");
        let right_coordinator_db = Db::new(&DbConfig {
            url: right_coordinator_db_url,
            migrate,
            create,
        })
        .await?;

        let mut right_participant_dbs = vec![];
        for (i, url) in right_participant_db_urls.into_iter().enumerate() {
            tracing::info!(participant=?i, "Connecting to right participant db");
            let db = Db::new(&DbConfig {
                url,
                migrate,
                create,
            })
            .await?;

            right_participant_dbs.push(db);
        }

        Ok(Self {
            left_coordinator_db,
            left_participant_dbs,
            right_coordinator_db,
            right_participant_dbs,
        })
    }

    #[tracing::instrument(skip(self,))]
    pub async fn prune_items(&self, serial_id: u64) -> eyre::Result<()> {
        self.left_coordinator_db.prune_items(serial_id).await?;
        self.right_coordinator_db.prune_items(serial_id).await?;

        for db in self.left_participant_dbs.iter() {
            db.prune_items(serial_id).await?;
        }

        for db in self.right_participant_dbs.iter() {
            db.prune_items(serial_id).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self,))]
    pub async fn fetch_latest_serial_id(&self) -> eyre::Result<u64> {
        let mut ids = Vec::new();

        ids.push(self.left_coordinator_db.fetch_latest_mask_id().await?);
        ids.push(self.right_coordinator_db.fetch_latest_mask_id().await?);

        for db in self.left_participant_dbs.iter() {
            ids.push(db.fetch_latest_share_id().await?);
        }

        for db in self.right_participant_dbs.iter() {
            ids.push(db.fetch_latest_share_id().await?);
        }

        ids.into_iter().min().context("No serial ids found")
    }
}
