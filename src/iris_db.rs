use futures::{Stream, TryStreamExt};
use mongodb::bson::doc;
use mongodb::Collection;
use serde::{Deserialize, Serialize};

use crate::bits::Bits;

pub const IRIS_CODE_BATCH_SIZE: u32 = 30_000;
pub const DATABASE_NAME: &str = "iris";
pub const COLLECTION_NAME: &str = "codes.v2";
pub const FINAL_RESULT_COLLECTION_NAME: &str = "iris.mpc.results";
pub const FINAL_RESULT_STATUS: &str = "COMPLETED";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
    pub serial_id: u64,
    pub iris_code_left: Bits,
    pub mask_code_left: Bits,
    pub iris_code_right: Bits,
    pub mask_code_right: Bits,
    pub whitelisted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinalResult {
    pub status: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial_id: Option<u64>,

    pub signup_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,

    pub right_result: SideResult,

    pub left_result: SideResult,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SideResult {
    // This struct is intentionally left empty to represent an empty document.
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
        let collection: Collection<IrisCodeEntry> =
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
    pub async fn stream_whitelisted_iris_codes(
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
