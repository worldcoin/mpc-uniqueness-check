use aws_sdk_sqs::types::Message;
use eyre::{Context, ContextCompat};
use serde::de::DeserializeOwned;

pub async fn sqs_dequeue_raw(
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

pub async fn sqs_dequeue<T>(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> eyre::Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let messages = sqs_dequeue_raw(client, queue_url).await?;

    messages
        .into_iter()
        .map(|msg| msg.body.context("Missing body"))
        .map(|body| {
            let body = body?;

            serde_json::from_str(&body).context("Failed to parse message")
        })
        .collect()
}
