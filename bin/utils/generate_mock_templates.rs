use clap::Args;

use crate::common::generate_templates;

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
    let templates = generate_templates(args.num_templates);
    let json = serde_json::to_string(&templates)?;

    //write to file
    std::fs::write(&args.output, json)?;

    Ok(())
}
