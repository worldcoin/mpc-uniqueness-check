use std::collections::HashSet;

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

    #[tracing::instrument(skip(
        self,
        serial_id,
        left_templates,
        right_templates
    ))]
    pub async fn insert_shares_and_masks(
        &self,
        mut serial_id: u64,
        left_templates: &[Template],
        right_templates: &[Template],
    ) -> eyre::Result<()> {
        let participants = self.left_participant_dbs.len();

        let mut left_participant_shares: Vec<Vec<(u64, EncodedBits)>> =
            Vec::with_capacity(participants);
        let mut right_participant_shares: Vec<Vec<(u64, EncodedBits)>> =
            Vec::with_capacity(participants);

        let mut left_masks = vec![];
        let mut right_masks = vec![];

        // Collect the shares and masks with the corresponding serial id
        for (left, right) in left_templates.iter().zip(right_templates) {
            let left_shares = mpc::distance::encode(left)
                .share(participants)
                .iter()
                .map(|share| (serial_id, *share))
                .collect::<Vec<(u64, EncodedBits)>>();

            for (i, share) in left_shares.into_iter().enumerate() {
                left_participant_shares[i].push(share);
            }

            left_masks.push((serial_id, left.mask));

            let right_shares = mpc::distance::encode(right)
                .share(participants)
                .iter()
                .map(|share| (serial_id, *share))
                .collect::<Vec<(u64, EncodedBits)>>();

            for (i, share) in right_shares.into_iter().enumerate() {
                right_participant_shares[i].push(share);
            }

            right_masks.push((serial_id, right.mask));

            serial_id += 1;
        }

        // Insert left shares and masks
        for (i, db) in self.left_participant_dbs.iter().enumerate() {
            db.insert_shares(&left_participant_shares[i]).await?;
        }
        self.left_coordinator_db.insert_masks(&left_masks).await?;

        // Insert right shares and masks
        for (i, db) in self.right_participant_dbs.iter().enumerate() {
            db.insert_shares(&right_participant_shares[i]).await?;
        }
        self.right_coordinator_db.insert_masks(&right_masks).await?;

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
