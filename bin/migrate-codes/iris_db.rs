use futures::TryStreamExt;
use mpc::bits::Bits;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
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

    pub async fn fetch_iris_codes(
        &self,
        mut from: u64,
        batch_size: i64,
    ) -> eyre::Result<Vec<IrisCodeEntry>> {
        let collection = self.db.collection("codes.v2");
        let total_items = collection.estimated_document_count(None).await?;

        let mut items = vec![];

        while from < total_items {
            let find_options = mongodb::options::FindOptions::builder()
                .skip(from as u64)
                .limit(batch_size as i64)
                .build();

            let mut cursor = collection.find(None, find_options).await?;

            while let Some(document) = cursor.try_next().await? {
                let iris_code_element =
                    mongodb::bson::from_document::<IrisCodeEntry>(document)?;
                items.push(iris_code_element);
            }

            from += batch_size as u64;
        }

        Ok(items)
    }
}
