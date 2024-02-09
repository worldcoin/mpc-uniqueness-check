use std::collections::HashMap;

use serde::Serialize;
use tinytemplate::TinyTemplate;

const TEMPLATE_NAME: &str = "TEMPLATE";

#[derive(Serialize)]
struct Context {
    env: HashMap<String, String>,
}

pub fn resolve_template(s: &str) -> eyre::Result<String> {
    let mut tt = TinyTemplate::new();

    tt.add_template(TEMPLATE_NAME, s)?;

    let context = Context {
        env: std::env::vars().collect(),
    };

    Ok(tt.render(TEMPLATE_NAME, &context)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_template() {
        let template = "Hello, {env.USER}!";
        let expected = format!("Hello, {}!", std::env::var("USER").unwrap());
        let actual = resolve_template(template).unwrap();

        assert_eq!(actual, expected);
    }
}
