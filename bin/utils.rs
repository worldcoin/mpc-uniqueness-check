use clap::{Args, Parser};
use mpc::template::Template;
use rand::{thread_rng, Rng};

#[derive(Debug, Clone, Parser)]
enum Opt {
    HttpQuery(RandomQuery),
    SQSQuery(RandomQuery),
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
        Opt::HttpQuery(args) => {
            let mut rng = thread_rng();

            let template: Template = rng.gen();

            reqwest::Client::new()
                .post(&format!("{}/", args.url))
                .json(&template)
                .send()
                .await?
                .error_for_status()?;
        }

        Opt::SQSQuery(args) => {
            let aws_config = aws_config::load_defaults(
                aws_config::BehaviorVersion::latest(),
            )
            .await;

            let aws_client = aws_sdk_sqs::Client::new(&aws_config);

            let mut rng = thread_rng();

            let template: Template = rng.gen();

            aws_client
                .send_message()
                .queue_url(args.url)
                .message_body(serde_json::to_string(&template)?)
                .send()
                .await?;
        }
    }

    Ok(())
}
