use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use mpc::bits::Bits;
use serde::{Deserialize, Serialize};

use crate::common::{generate_random_string, generate_templates};

pub const DATABASE_NAME: &str = "iris";
pub const COLLECTION_NAME: &str = "codes.v2";

#[derive(Debug, Clone, Args)]
pub struct SeedIrisDb {
    #[clap(short, long)]
    pub iris_code_db: String,

    #[clap(short, long)]
    pub num_templates: usize,

    #[clap(short, long, default_value = "100")]
    pub batch_size: usize,
}

pub async fn seed_iris_db(args: &SeedIrisDb) -> eyre::Result<()> {
    let client_options =
        mongodb::options::ClientOptions::parse(&args.iris_code_db).await?;

    let client: mongodb::Client =
        mongodb::Client::with_options(client_options)?;

    let iris_db = client.database(DATABASE_NAME);

    tracing::info!("Generating left templates...");
    let left_templates = generate_templates(args.num_templates);
    tracing::info!("Generating right templates...");
    let right_templates = generate_templates(args.num_templates);

    let collection = iris_db.collection::<IrisCodeEntry>(COLLECTION_NAME);

    // Next serial id with 1 based indexing
    let next_serial_id = collection.count_documents(None, None).await? + 1;

    tracing::info!(?next_serial_id);

    let documents = left_templates
        .iter()
        .zip(right_templates.iter())
        .enumerate()
        .map(|(serial_id, (left, right))| IrisCodeEntry {
            signup_id: generate_random_string(10),
            mpc_serial_id: next_serial_id + serial_id as u64,
            iris_code_left: left.code,
            mask_code_left: left.mask,
            iris_code_right: right.code,
            mask_code_right: right.mask,
            whitelisted: true,
        })
        .collect::<Vec<IrisCodeEntry>>();

    let pb = ProgressBar::new(documents.len() as u64)
        .with_message("Seeding iris codes");

    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar"));

    for chunk in documents.chunks(args.batch_size) {
        collection.insert_many(chunk, None).await?;

        pb.inc(chunk.len() as u64);
    }

    pb.finish_with_message("Seeded iris codes");

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct IrisCodeEntry {
    pub signup_id: String,
    pub mpc_serial_id: u64,
    pub iris_code_left: Bits,
    pub mask_code_left: Bits,
    pub iris_code_right: Bits,
    pub mask_code_right: Bits,
    pub whitelisted: bool,
}
