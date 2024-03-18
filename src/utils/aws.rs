use std::collections::HashMap;
use std::fmt::Debug;

use aws_config::Region;
use aws_sdk_sqs::types::{Message, MessageAttributeValue, QueueAttributeName};
use eyre::Context;
use opentelemetry::trace::{
    SpanContext, SpanId, TraceFlags, TraceId, TraceState,
};
use serde::Serialize;

use crate::config::AwsConfig;

const DEQUEUE_WAIT_TIME_SECONDS: i32 = 1;
const TRACE_ID_MESSAGE_ATTRIBUTE_NAME: &str = "TraceID";
const SPAN_ID_MESSAGE_ATTRIBUTE_NAME: &str = "SpanID";

pub async fn sqs_client_from_config(
    config: &AwsConfig,
) -> eyre::Result<aws_sdk_sqs::Client> {
    let mut config_builder =
        aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(endpoint_url) = config.endpoint.as_ref() {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    if let Some(region) = config.region.as_ref() {
        config_builder = config_builder.region(Region::new(region.clone()));
    }

    let aws_config = config_builder.load().await;

    let aws_client = aws_sdk_sqs::Client::new(&aws_config);

    Ok(aws_client)
}

#[tracing::instrument(skip(client, queue_url))]
pub async fn check_approximate_queue_length(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> eyre::Result<usize> {
    let queue_attributes = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await?;

    let message_count = queue_attributes
        .attributes()
        .and_then(|attrs| {
            attrs.get(&QueueAttributeName::ApproximateNumberOfMessages)
        })
        .and_then(|count| count.parse::<usize>().ok())
        .unwrap_or(0);

    Ok(message_count)
}

#[tracing::instrument(skip(client, queue_url))]
pub async fn sqs_dequeue(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> eyre::Result<Vec<Message>> {
    let sqs_message = client
        .receive_message()
        .message_attribute_names("All")
        .queue_url(queue_url)
        .wait_time_seconds(DEQUEUE_WAIT_TIME_SECONDS)
        .send()
        .await?;

    let messages = sqs_message.messages;

    let Some(messages) = messages else {
        return Ok(vec![]);
    };

    let message_receipts = messages
        .iter()
        .map(|message| message.receipt_handle.clone())
        .collect::<Vec<Option<String>>>();

    tracing::info!(?message_receipts, "Dequeued messages");

    Ok(messages)
}

#[tracing::instrument(skip(client, payload))]
pub async fn sqs_enqueue<T>(
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    message_group_id: &str,
    payload: T,
) -> eyre::Result<()>
where
    T: Serialize + Debug,
{
    let body = serde_json::to_string(&payload)
        .wrap_err("Failed to serialize message")?;

    let message_attributes = construct_message_attributes()?;

    let send_message_output = client
        .send_message()
        .queue_url(queue_url)
        .message_group_id(message_group_id)
        .set_message_attributes(Some(message_attributes))
        .message_body(body)
        .send()
        .await?;

    tracing::info!(?send_message_output, ?payload, "Enqueued message");

    Ok(())
}

pub fn construct_message_attributes(
) -> eyre::Result<HashMap<String, MessageAttributeValue>> {
    let (trace_id, span_id) = telemetry_batteries::tracing::extract_span_ids();

    let mut message_attributes = HashMap::new();

    let trace_id_message_attribute = MessageAttributeValue::builder()
        .data_type("String")
        .string_value(trace_id.to_string())
        .build()?;

    message_attributes.insert(
        TRACE_ID_MESSAGE_ATTRIBUTE_NAME.to_string(),
        trace_id_message_attribute,
    );

    let span_id_message_attribute = MessageAttributeValue::builder()
        .data_type("String")
        .string_value(span_id.to_string())
        .build()?;

    message_attributes.insert(
        SPAN_ID_MESSAGE_ATTRIBUTE_NAME.to_string(),
        span_id_message_attribute,
    );

    Ok(message_attributes)
}

pub async fn sqs_delete_message(
    client: &aws_sdk_sqs::Client,
    queue_url: impl Into<String>,
    receipt_handle: impl Into<String>,
) -> eyre::Result<()> {
    let receipt_handle = receipt_handle.into();

    client
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(&receipt_handle)
        .send()
        .await?;

    tracing::info!(?receipt_handle, "Deleted message from queue");

    Ok(())
}

pub fn trace_from_message_attributes(
    message_attributes: &HashMap<String, MessageAttributeValue>,
    receipt_handle: &str,
) -> eyre::Result<()> {
    if let Some(trace_id) =
        message_attributes.get(TRACE_ID_MESSAGE_ATTRIBUTE_NAME)
    {
        if let Some(span_id) =
            message_attributes.get(SPAN_ID_MESSAGE_ATTRIBUTE_NAME)
        {
            let trace_id = trace_id
                .string_value()
                .expect("Could not parse TraceID")
                .parse::<u128>()?;

            let span_id = span_id
                .string_value()
                .expect("Could not parse SpanID")
                .parse::<u64>()?;

            // Create and set the span parent context
            let parent_ctx = SpanContext::new(
                TraceId::from(trace_id),
                SpanId::from(span_id),
                TraceFlags::default(),
                true,
                TraceState::default(),
            );

            telemetry_batteries::tracing::trace_from_ctx(parent_ctx);
        } else {
            tracing::warn!(?receipt_handle, "SQS message missing SpanID");
        }
    } else {
        tracing::warn!(?receipt_handle, "SQS message missing TraceID");
    }

    Ok(())
}
