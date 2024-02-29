use clap::Args;
use mpc::bits::Bits;
use mpc::rng_source::RngSource;
use mpc::template::Template;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::generate_random_string;

pub const DATABASE_NAME: &str = "iris";
pub const COLLECTION_NAME: &str = "codes.v2";

#[derive(Debug, Clone, Args)]
pub struct SeedIrisDb {
    #[clap(short, long)]
    pub iris_code_db: String,

    #[clap(short, long)]
    pub num_templates: usize,

    #[clap(short, long, default_value = "10000")]
    pub batch_size: usize,

    #[clap(short, long, env, default_value = "thread")]
    pub rng: RngSource,
}

pub async fn seed_iris_db(args: &SeedIrisDb) -> eyre::Result<()> {
    let client_options =
        mongodb::options::ClientOptions::parse(&args.iris_code_db).await?;

    let client: mongodb::Client =
        mongodb::Client::with_options(client_options)?;

    let iris_db = client.database(DATABASE_NAME);

    let mut rng = args.rng.to_rng();

    tracing::info!("Generating codes");
    let left_templates = (0..args.num_templates)
        .map(|_| rng.gen())
        .collect::<Vec<Template>>();

    let right_templates = (0..args.num_templates)
        .map(|_| rng.gen())
        .collect::<Vec<Template>>();

    let collection = iris_db.collection::<IrisCodeEntry>(COLLECTION_NAME);
    let next_serial_id = collection.count_documents(None, None).await?;

    let documents = left_templates
        .iter()
        .zip(right_templates.iter())
        .enumerate()
        .map(|(serial_id, (left, right))| IrisCodeEntry {
            signup_id: generate_random_string(10),
            serial_id: next_serial_id + serial_id as u64,
            iris_code_left: left.code,
            mask_code_left: left.mask,
            iris_code_right: right.code,
            mask_code_right: right.mask,
        })
        .collect::<Vec<IrisCodeEntry>>();

    for (i, chunk) in documents.chunks(args.batch_size).enumerate() {
        tracing::info!(
            "Seeding iris codes, chunk {}/{}",
            i + 1,
            (documents.len() / args.batch_size) + 1
        );
        collection.insert_many(chunk, None).await?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
    pub serial_id: u64,
    pub iris_code_left: Bits,
    pub mask_code_left: Bits,
    pub iris_code_right: Bits,
    pub mask_code_right: Bits,
}
