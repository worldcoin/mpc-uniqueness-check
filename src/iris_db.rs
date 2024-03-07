use futures::{Stream, TryStreamExt};
use mongodb::bson::doc;
use mongodb::Collection;
use serde::{Deserialize, Serialize};

use crate::bits::Bits;

pub const IRIS_CODE_BATCH_SIZE: u32 = 30_000;
pub const DATABASE_NAME: &str = "iris";
pub const COLLECTION_NAME: &str = "codes.v2";
pub const FINAL_RESULT_COLLECTION_NAME: &str = "mpc.results";
pub const FINAL_RESULT_STATUS: &str = "COMPLETED";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
    /// Internal serial id of the iris code db
    pub serial_id: u64,
    pub iris_code_left: Bits,
    pub mask_code_left: Bits,
    pub iris_code_right: Bits,
    pub mask_code_right: Bits,
    pub whitelisted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FinalResult {
    /// Should always by "COMPLETED"
    pub status: String,

    /// The MPC serial id associated with this signup
    pub serial_id: u64,

    /// A unique signup id string
    pub signup_id: String,

    pub unique: bool,

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
    pub async fn save_final_results(
        &self,
        final_results: &[FinalResult],
    ) -> eyre::Result<()> {
        let collection: Collection<FinalResult> =
            self.db.collection(FINAL_RESULT_COLLECTION_NAME);

        collection.insert_many(final_results, None).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_final_result_by_serial_id(
        &self,
        serial_id: u64,
    ) -> eyre::Result<Option<FinalResult>> {
        let collection: Collection<FinalResult> =
            self.db.collection(FINAL_RESULT_COLLECTION_NAME);

        let final_result = collection
            .find_one(doc! { "serial_id": serial_id as i64 }, None)
            .await?;

        Ok(final_result)
    }

    /// Removes all final result entries with serial id larger than the given one
    #[tracing::instrument(skip(self))]
    pub async fn prune_final_results(
        &self,
        serial_id: u64,
    ) -> eyre::Result<()> {
        let collection: Collection<FinalResult> =
            self.db.collection(FINAL_RESULT_COLLECTION_NAME);

        collection
            .delete_many(
                doc! { "serial_id": { "$gt": serial_id as i64 } },
                None,
            )
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn count_whitelisted_iris_codes(
        &self,
        last_serial_id: u64,
    ) -> eyre::Result<u64> {
        let collection: Collection<IrisCodeEntry> =
            self.db.collection(COLLECTION_NAME);

        let count = collection
            .count_documents(
                doc! {
                    "serial_id": {"$gt": last_serial_id as i64},
                    "whitelisted": true,
                },
                None,
            )
            .await?;

        Ok(count)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_entry_by_signup_id(
        &self,
        signup_id: &str,
    ) -> eyre::Result<Option<IrisCodeEntry>> {
        let collection: Collection<IrisCodeEntry> =
            self.db.collection(COLLECTION_NAME);

        let iris_code_entry = collection
            .find_one(doc! {"signup_id": signup_id}, None)
            .await?;

        Ok(iris_code_entry)
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
            .sort(doc! { "serial_id": 1 })
            .build();

        let collection = self.db.collection(COLLECTION_NAME);

        let cursor = collection
            .find(
                doc! {
                    "serial_id": {"$gt": last_serial_id as i64},
                    "whitelisted": true
                },
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
