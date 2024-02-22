use futures::TryStreamExt;
use mpc::bits::Bits;
use serde::{Deserialize, Serialize};

use crate::IRIS_CODE_BATCH_SIZE;

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
    pub async fn new(url: &str) -> eyre::Result<Self> {
        let client_options =
            mongodb::options::ClientOptions::parse(url).await?;

        let client: mongodb::Client =
            mongodb::Client::with_options(client_options)?;

        let db = client.database("iris");

        Ok(Self { db })
    }

    pub async fn get_iris_code_snapshot(
        &self,
    ) -> eyre::Result<Vec<IrisCodeEntry>> {
        let collection = self.db.collection("codes.v2");

        let mut items = vec![];

        let find_options = mongodb::options::FindOptions::builder()
            .limit(IRIS_CODE_BATCH_SIZE) // Ensure this constant is defined somewhere
            .build();

        let mut cursor = collection.find(None, find_options).await?;

        while let Some(document) = cursor.try_next().await? {
            let iris_code_element =
                mongodb::bson::from_document::<IrisCodeEntry>(document)?;
            items.push(iris_code_element);
        }

        Ok(items)
    }
}
