use std::collections::HashSet;

use mpc::bits::Bits;
use mpc::config::DbConfig;
use mpc::db::Db;
use mpc::distance::EncodedBits;
use mpc::template::Template;

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
    ) -> eyre::Result<Self> {
        tracing::info!("Connecting to left coordinator db");
        let left_coordinator_db = Db::new(&DbConfig {
            url: left_coordinator_db_url,
            migrate: false,
            create: false,
        })
        .await?;

        let mut left_participant_dbs = vec![];

        for (i, url) in left_participant_db_urls.into_iter().enumerate() {
            tracing::info!(participant=?i, "Connecting to left participant db");
            let db = Db::new(&DbConfig {
                url,
                migrate: false,
                create: false,
            })
            .await?;
            left_participant_dbs.push(db);
        }

        tracing::info!("Connecting to right coordinator db");
        let right_coordinator_db = Db::new(&DbConfig {
            url: right_coordinator_db_url,
            migrate: false,
            create: false,
        })
        .await?;

        let mut right_participant_dbs = vec![];
        for (i, url) in right_participant_db_urls.into_iter().enumerate() {
            tracing::info!(participant=?i, "Connecting to right participant db");
            let db = Db::new(&DbConfig {
                url,
                migrate: false,
                create: false,
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
        let mut ids = HashSet::new();

        let left_coordinator_id =
            self.left_coordinator_db.fetch_latest_mask_id().await?;

        tracing::info!(?left_coordinator_id, "Latest left mask Id");

        let right_coordinator_id =
            self.right_coordinator_db.fetch_latest_share_id().await?;

        tracing::info!(?right_coordinator_id, "Latest right mask Id");

        for (i, db) in self.left_participant_dbs.iter().enumerate() {
            let id = db.fetch_latest_share_id().await?;
            tracing::info!(?id, participant=?i, "Latest left share Id");
            ids.insert(id);
        }

        for (i, db) in self.right_participant_dbs.iter().enumerate() {
            let id = db.fetch_latest_share_id().await?;
            tracing::info!(?id, participant=?i, "Latest right share Id");
            ids.insert(id);
        }

        if ids.len() != 1 {
            return Err(eyre::eyre!("Mismatched serial ids"));
        }

        Ok(left_coordinator_id)
    }
}
