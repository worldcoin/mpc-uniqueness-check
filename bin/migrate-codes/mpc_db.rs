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
        let left_coordinator_db = Db::new(&DbConfig {
            url: left_coordinator_db_url,
            migrate: false,
            create: false,
        })
        .await?;

        let mut left_participant_dbs = vec![];

        for url in left_participant_db_urls {
            let db = Db::new(&DbConfig {
                url,
                migrate: false,
                create: false,
            })
            .await?;
            left_participant_dbs.push(db);
        }

        let right_coordinator_db = Db::new(&DbConfig {
            url: right_coordinator_db_url,
            migrate: false,
            create: false,
        })
        .await?;

        let mut right_participant_dbs = vec![];
        for url in right_participant_db_urls {
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
}
