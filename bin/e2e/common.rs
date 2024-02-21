use std::time::Duration;

use aws_sdk_sqs::types::{Message, QueueAttributeName};
use eyre::ContextCompat;
use mpc::coordinator::{self, UniquenessCheckRequest};
use mpc::encoded_bits::EncodedBits;
use mpc::participant;
use mpc::template::Template;
use rand::distributions::Alphanumeric;
use rand::Rng;

pub async fn wait_for_queues(
    sqs_client: &aws_sdk_sqs::Client,
    queues: Vec<&str>,
) -> eyre::Result<()> {
    for queue in queues {
        tracing::info!(?queue, "Waiting for queue");
        loop {
            let Ok(response) = sqs_client
                .get_queue_attributes()
                .queue_url(queue)
                .attribute_names(
                    QueueAttributeName::ApproximateNumberOfMessages,
                )
                .send()
                .await
            else {
                continue;
            };

            let Some(attributes) = response.attributes else {
                continue;
            };

            let Some(_num_messages) = attributes
                .get(&QueueAttributeName::ApproximateNumberOfMessages)
            else {
                continue;
            };

            break;
        }
    }

    Ok(())
}

pub async fn send_query(
    template: Template,
    sqs_client: &aws_sdk_sqs::Client,
    query_queue: &str,
    signup_id: &str,
    group_id: &str,
) -> eyre::Result<()> {
    tracing::info!(?signup_id, ?group_id, "Sending request");

    let request = UniquenessCheckRequest {
        plain_code: template,
        signup_id: signup_id.to_string(),
    };

    sqs_client
        .send_message()
        .queue_url(query_queue)
        .message_group_id(group_id)
        .message_body(serde_json::to_string(&request)?)
        .send()
        .await?;

    Ok(())
}

pub async fn receive_result(
    sqs_client: &aws_sdk_sqs::Client,
    results_queue: &str,
) -> eyre::Result<Message> {
    wait_for_messages(sqs_client, results_queue).await?;

    let messages = sqs_client
        .receive_message()
        .queue_url(results_queue)
        .max_number_of_messages(1)
        .send()
        .await?;

    let message = messages.messages.context("No messages found")?.remove(0);

    tracing::info!(?message, "Message received from queue");

    Ok(message)
}

pub async fn wait_for_messages(
    sqs_client: &aws_sdk_sqs::Client,
    results_queue: &str,
) -> eyre::Result<()> {
    loop {
        let queue_attributes = sqs_client
            .get_queue_attributes()
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .queue_url(results_queue)
            .send()
            .await?;

        let attributes = queue_attributes
            .attributes
            .expect("Could not get queue attributes ");

        let approx_num_messages = attributes
            .get(&QueueAttributeName::ApproximateNumberOfMessages)
            .expect("Could not get approximate number of messages in queue");

        if approx_num_messages == "0" {
            tracing::info!("No messages in queue, retrying");
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
            continue;
        } else {
            return Ok(());
        }
    }
}

pub async fn seed_db_sync(
    sqs_client: &aws_sdk_sqs::Client,
    coordinator_db_sync_queue: &str,
    participant_db_sync_queues: &[&str],
    template: Template,
    serial_id: u64,
) -> eyre::Result<()> {
    tracing::info!("Encoding shares");
    let shares: Box<[EncodedBits]> = mpc::distance::encode(&template)
        .share(participant_db_sync_queues.len());

    let coordinator_payload =
        serde_json::to_string(&vec![coordinator::DbSyncPayload {
            id: serial_id,
            mask: template.mask,
        }])?;

    tracing::info!(
        "Sending {} bytes to coordinator db sync queue",
        coordinator_payload.len()
    );

    sqs_client
        .send_message()
        .queue_url(coordinator_db_sync_queue)
        .message_body(coordinator_payload)
        .send()
        .await?;

    for (i, participant_queue) in participant_db_sync_queues.iter().enumerate()
    {
        let participant_payload =
            serde_json::to_string(&vec![participant::DbSyncPayload {
                id: serial_id,
                share: shares[i],
            }])?;

        tracing::info!(
            "Sending {} bytes to participant db sync queue",
            participant_payload.len()
        );

        sqs_client
            .send_message()
            .queue_url(*participant_queue)
            .message_body(participant_payload)
            .send()
            .await?;
    }

    tracing::info!("Waiting for 300 ms for db sync to propagate");
    tokio::time::sleep(Duration::from_millis(300)).await;

    Ok(())
}

pub fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
