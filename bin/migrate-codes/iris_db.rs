use futures::{Stream, TryStreamExt};
use mongodb::bson::{doc, Document};
use mongodb::Collection;
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
    pub async fn count_iris_codes(
        &self,
        last_serial_id: u64,
    ) -> eyre::Result<u64> {
        let collection: Collection<Document> =
            self.db.collection(COLLECTION_NAME);

        let count = collection
            .count_documents(
                doc! {"mpc_serial_id": {"$gt": last_serial_id as i64}},
                None,
            )
            .await?;

        Ok(count)
    }

    #[tracing::instrument(skip(self))]
    pub async fn stream_iris_codes(
        &self,
        last_serial_id: u64,
    ) -> eyre::Result<
        impl Stream<Item = Result<IrisCodeEntry, mongodb::error::Error>>,
    > {
        let find_options = mongodb::options::FindOptions::builder()
            .batch_size(IRIS_CODE_BATCH_SIZE)
            .sort(doc! {"mpc_serial_id": 1})
            .build();

        let collection = self.db.collection(COLLECTION_NAME);

        let cursor = collection
            .find(
                doc! {"mpc_serial_id": {"$gt": last_serial_id as i64}},
                find_options,
            )
            .await?;

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
            .sort(doc! {"mpc_serial_id": 1})
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
