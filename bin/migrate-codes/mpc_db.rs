use eyre::ContextCompat;
use mpc::bits::Bits;
use mpc::config::DbConfig;
use mpc::db::Db;
use mpc::distance::EncodedBits;

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

    #[tracing::instrument(skip(self))]
    pub async fn insert_shares_and_masks(
        &self,
        left_data: Vec<(u64, Bits, Box<[EncodedBits]>)>,
        right_data: Vec<(u64, Bits, Box<[EncodedBits]>)>,
    ) -> eyre::Result<()> {
        //TODO: logging for progress

        let (left_masks, left_shares): (
            Vec<(u64, Bits)>,
            Vec<Vec<(u64, EncodedBits)>>,
        ) = left_data
            .into_iter()
            .map(|(id, mask, shares)| {
                let shares: Vec<(u64, EncodedBits)> =
                    shares.into_iter().map(|share| (id, *share)).collect();

                ((id, mask), shares)
            })
            .unzip();

        let (right_masks, right_shares): (
            Vec<(u64, Bits)>,
            Vec<Vec<(u64, EncodedBits)>>,
        ) = right_data
            .into_iter()
            .map(|(id, mask, shares)| {
                let shares: Vec<(u64, EncodedBits)> =
                    shares.into_iter().map(|share| (id, *share)).collect();

                ((id, mask), shares)
            })
            .unzip();

        let coordinator_tasks = vec![
            self.left_coordinator_db.insert_masks(&left_masks),
            self.right_coordinator_db.insert_masks(&right_masks),
        ];

        let participant_tasks = self
            .left_participant_dbs
            .iter()
            .zip(left_shares.iter())
            .chain(self.right_participant_dbs.iter().zip(right_shares.iter()))
            .map(|(db, shares)| db.insert_shares(shares));

        for task in coordinator_tasks {
            task.await?;
        }

        for task in participant_tasks {
            task.await?;
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

        Ok(ids.into_iter().min().context("No serial ids found")?)
    }
}
