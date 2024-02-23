use clap::Args;
use mpc::template::Template;
use rand::{thread_rng, Rng};

#[derive(Debug, Clone, Args)]
pub struct GenerateMockTemplates {
    #[clap(short, long)]
    pub output: String,

    #[clap(short, long)]
    pub num_templates: usize,
}

pub async fn generate_mock_templates(
    args: &GenerateMockTemplates,
) -> eyre::Result<()> {
    let mut rng = thread_rng();

    let templates: Vec<Template> =
        (0..args.num_templates).map(|_| rng.gen()).collect();

    let json = serde_json::to_string(&templates)?;

    //write to file
    std::fs::write(&args.output, json)?;

    Ok(())
}
