use clap::{Args, Parser};
use mpc::template::Template;
use rand::{thread_rng, Rng};

#[derive(Debug, Clone, Parser)]
enum Opt {
    RandomQuery(RandomQuery),
}

#[derive(Debug, Clone, Args)]
struct RandomQuery {
    #[clap(short, long, default_value = "http://localhost:8080")]
    pub url: String,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenv::dotenv().ok();

    let args = Opt::parse();

    match args {
        Opt::RandomQuery(args) => {
            let mut rng = thread_rng();

            let template: Template = rng.gen();

            reqwest::Client::new()
                .post(&format!("{}/", args.url))
                .json(&template)
                .send()
                .await?
                .error_for_status()?;
        }
    }

    Ok(())
}
