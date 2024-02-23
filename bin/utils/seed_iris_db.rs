use clap::Args;
use mpc::bits::Bits;
use mpc::template::Template;
use rand::{thread_rng, Rng};
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
}

pub async fn seed_iris_db(args: &SeedIrisDb) -> eyre::Result<()> {
    let client_options =
        mongodb::options::ClientOptions::parse(&args.iris_code_db).await?;

    let client: mongodb::Client =
        mongodb::Client::with_options(client_options)?;

    let iris_db = client.database(DATABASE_NAME);

    let mut rng = thread_rng();

    let left_templates = (0..args.num_templates)
        .map(|_| rng.gen())
        .collect::<Vec<Template>>();

    let right_templates = (0..args.num_templates)
        .map(|_| rng.gen())
        .collect::<Vec<Template>>();

    let collection = iris_db.collection::<IrisCodeEntry>(COLLECTION_NAME);

    //TODO: update to insert many in batches
    for (serial_id, (left, right)) in left_templates
        .iter()
        .zip(right_templates.iter())
        .enumerate()
    {
        let iris_code_entry = IrisCodeEntry {
            signup_id: generate_random_string(10),
            serial_id: serial_id as u64,
            iris_code_left: left.code,
            mask_code_left: left.mask,
            iris_code_right: right.code,
            mask_code_right: right.mask,
        };

        collection.insert_one(iris_code_entry, None).await?;
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
