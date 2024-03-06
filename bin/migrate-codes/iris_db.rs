use futures::{Stream, TryStreamExt};
use mongodb::bson::doc;
use mpc::bits::Bits;
use serde::{Deserialize, Serialize};

pub const IRIS_CODE_BATCH_SIZE: u32 = 30_000;
pub const DATABASE_NAME: &str = "iris";
pub const COLLECTION_NAME: &str = "codes.v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
    pub mpc_serial_id: u64,
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

    #[tracing::instrument(skip(self))]
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

                last_serial_id += iris_code_element.mpc_serial_id as i64;

                items.push(iris_code_element);

                items_added += 1;
            }

            if items_added == 0 {
                break;
            }
        }

        items.sort_by(|a, b| a.mpc_serial_id.cmp(&b.mpc_serial_id));

        Ok(items)
    }

    #[tracing::instrument(skip(self))]
    pub async fn stream_iris_codes(
        &self,
    ) -> eyre::Result<
        impl Stream<Item = Result<IrisCodeEntry, mongodb::error::Error>>,
    > {
        let find_options = mongodb::options::FindOptions::builder()
            .batch_size(IRIS_CODE_BATCH_SIZE)
            .sort(doc! {"serial_id": 1})
            .build();

        let collection = self.db.collection(COLLECTION_NAME);

        let cursor = collection.find(doc! {}, find_options).await?;
        let codes_stream = cursor.and_then(|document| async move {
            let iris_code_element =
                mongodb::bson::from_document::<IrisCodeEntry>(document)?;

            eyre::Result::Ok(iris_code_element)
        });

        Ok(codes_stream)
    }
}

#[cfg(test)]
mod tests {
    use futures::{pin_mut, StreamExt};

    use super::*;

    #[tokio::test]
    async fn whatever() -> eyre::Result<()> {
        std::env::set_var("RUST_LOG", "info");
        tracing_subscriber::fmt::init();

        let client_options = mongodb::options::ClientOptions::parse(
            "mongodb://admin:password@localhost:27017",
        )
        .await?;

        let client: mongodb::Client =
            mongodb::Client::with_options(client_options)?;

        let db = client.database(DATABASE_NAME);

        let find_options = mongodb::options::FindOptions::builder()
            .batch_size(IRIS_CODE_BATCH_SIZE)
            .sort(doc! {"serial_id": 1})
            .build();

        let collection = db.collection(COLLECTION_NAME);

        let mut cursor = collection.find(doc! {}, find_options).await?;

        let mut codes_stream = cursor.and_then(|document| async move {
            let iris_code_element =
                mongodb::bson::from_document::<IrisCodeEntry>(document)?;

            eyre::Result::Ok(iris_code_element)
        });

        pin_mut!(codes_stream);

        // Consume the stream
        while let Some(result) = codes_stream.next().await {
            tracing::info!(?result);
        }

        Ok(())
    }
}
