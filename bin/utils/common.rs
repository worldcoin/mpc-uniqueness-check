use indicatif::{ProgressBar, ProgressStyle};
use mpc::template::Template;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub fn generate_templates(num_templates: usize) -> Vec<Template> {
    let pb = ProgressBar::new(num_templates as u64)
        .with_message("Generating templates...");

    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar:.green}] {pos:>7}/{len:7} ({eta})")
        .expect("Could not create progress bar"));

    // Generate templates
    let templates = (0..num_templates)
        .into_par_iter()
        .map(|_| {
            let mut rng = thread_rng();

            let template = rng.gen();

            pb.inc(1);
            template
        })
        .collect::<Vec<Template>>();

    pb.finish_with_message("Created templates");

    templates
}

pub fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
