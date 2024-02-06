use aws_sdk_sqs::types::Message;
use eyre::{Context, ContextCompat};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::config::AwsConfig;

pub async fn sqs_client_from_config(
    config: &AwsConfig,
) -> eyre::Result<aws_sdk_sqs::Client> {
    let mut config_builder =
        aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(endpoint_url) = config.endpoint.as_ref() {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    let aws_config = config_builder.load().await;

    let aws_client = aws_sdk_sqs::Client::new(&aws_config);

    Ok(aws_client)
}

pub async fn sqs_dequeue(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> eyre::Result<Vec<Message>> {
    let messages = client
        .receive_message()
        .queue_url(queue_url)
        .send()
        .await?
        .messages;

    let Some(messages) = messages else {
        return Ok(vec![]);
    };

    Ok(messages)
}

pub async fn sqs_enqueue<T>(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    message: T,
) -> eyre::Result<()>
where
    T: Serialize,
{
    let body = serde_json::to_string(&message)
        .wrap_err("Failed to serialize message")?;

    client
        .send_message()
        .queue_url(queue_url)
        .message_body(body)
        .send()
        .await?;

    Ok(())
}

pub async fn sqs_delete_message(
    client: &aws_sdk_sqs::Client,
    queue_url: impl Into<String>,
    receipt_handle: impl Into<String>,
) -> eyre::Result<()> {
    client
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .send()
        .await?;

    Ok(())
}
