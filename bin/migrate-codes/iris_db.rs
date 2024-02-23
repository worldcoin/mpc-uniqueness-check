use futures::TryStreamExt;
use mongodb::bson::doc;
use mpc::bits::Bits;
use serde::{Deserialize, Serialize};

pub const IRIS_CODE_BATCH_SIZE: u32 = 30_000;
pub const DATABASE_NAME: &str = "iris";
pub const COLLECTION_NAME: &str = "codes.v2";

#[derive(Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
    pub serial_id: u64,
    pub iris_code_left: Bits,
    pub mask_code_left: Bits,
    pub iris_code_right: Bits,
    pub mask_code_right: Bits,
}

pub struct IrisDb {
    pub db: mongodb::Database,
}

impl IrisDb {
    pub async fn new(url: String) -> eyre::Result<Self> {
        let client_options =
            mongodb::options::ClientOptions::parse(url).await?;

        let client: mongodb::Client =
            mongodb::Client::with_options(client_options)?;

        let db = client.database(DATABASE_NAME);

        Ok(Self { db })
    }

    pub async fn get_iris_code_snapshot(
        &self,
    ) -> eyre::Result<Vec<IrisCodeEntry>> {
        let mut items = vec![];

        let mut last_serial_id = 0_i64;

        let collection = self.db.collection(COLLECTION_NAME);

        loop {
            let find_options = mongodb::options::FindOptions::builder()
                .batch_size(IRIS_CODE_BATCH_SIZE)
                .sort(doc! {"serial_id": 1})
                .build();

            let mut cursor = collection
                .find(doc! {"serial_id": {"$gt": last_serial_id}}, find_options)
                .await?;

            let mut items_added = 0;
            while let Some(document) = cursor.try_next().await? {
                let iris_code_element =
                    mongodb::bson::from_document::<IrisCodeEntry>(document)?;

                last_serial_id += iris_code_element.serial_id as i64;

                items.push(iris_code_element);

                items_added += 1;
            }

            if items_added == 0 {
                break;
            }
        }

        items.sort_by(|a, b| a.serial_id.cmp(&b.serial_id));

        Ok(items)
    }
}
