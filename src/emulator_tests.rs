//! The start of a notification pull against the Pub/Sub emulator
//! (testcontainers): a missing subscription fails the open, checked or not,
//! and a notification that comes while the start watches the stream is
//! received first.
//!
//! Gated behind the `integration-tests` feature:
//!   cargo test -p mcpg-plugin-storage-gcs --features integration-tests --lib

use gcloud_googleapis::pubsub::v1::PubsubMessage;
use gcloud_pubsub::subscription::SubscriptionConfig;
use mcpg_testcontainer_support::{
    NoEgress, PUBSUB_EMULATOR_PORT, pubsub_emulator, start_with_retry_async,
};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use super::*;

/// An emulator with topic `gcs-events` and its subscription `gcs-sub`, and
/// its `host:port`.
async fn emulator() -> (ContainerAsync<GenericImage>, String) {
    let container =
        start_with_retry_async("pubsub", || pubsub_emulator().no_egress().start()).await;
    let port = container
        .get_host_port_ipv4(PUBSUB_EMULATOR_PORT)
        .await
        .expect("port");
    let host = format!("127.0.0.1:{port}");
    let admin = client(&host).await;
    admin
        .create_topic("gcs-events", None, None)
        .await
        .expect("create topic gcs-events");
    admin
        .create_subscription("gcs-sub", "gcs-events", SubscriptionConfig::default(), None)
        .await
        .expect("create subscription gcs-sub");
    (container, host)
}

/// The watch's subscription `id` on the emulator at `host`.
fn subscription(host: &str, id: &str) -> Subscription {
    Subscription {
        name: id.to_owned(),
        project: Some("test-project".into()),
        credentials_json: None,
        emulator_host: Some(host.to_owned()),
        deadline_secs: 60,
        max_outstanding: 16,
    }
}

/// A new StreamingPull on subscription `id`, with the watch's settings, and
/// its full name.
async fn subscribe(client: &gcloud_pubsub::client::Client, id: &str) -> (String, MessageStream) {
    let subscription = client.subscription(id);
    let stream = subscription
        .subscribe(Some(subscribe_config(60, 16)))
        .await
        .expect("subscribe opens the stream");
    (subscription.fully_qualified_name().to_owned(), stream)
}

async fn client(host: &str) -> gcloud_pubsub::client::Client {
    let config = subscription(host, "gcs-sub")
        .config()
        .await
        .expect("an emulator configuration");
    gcloud_pubsub::client::Client::new(config)
        .await
        .expect("a client")
}

fn refused(name: &str) -> Option<Unconfirmed> {
    Some(Unconfirmed {
        name: name.to_owned(),
        refusal: "User not authorized to perform this action.".to_owned(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_subscription_fails_the_open_checked_or_not() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let (_container, host) = emulator().await;

    let checked = subscription(&host, "no-such-sub").open().await;
    assert_eq!(
        checked.err().as_deref(),
        Some("Pub/Sub subscription projects/test-project/subscriptions/no-such-sub does not exist"),
        "the emulator answers the check"
    );
    let Ok(open) = subscription(&host, "gcs-sub").open().await else {
        panic!("the open of an existing subscription fails");
    };
    open.dispose().await;

    let client = client(&host).await;
    let (name, stream) = subscribe(&client, "no-such-sub").await;
    let error = Pull::start(stream, refused(&name))
        .await
        .err()
        .expect("the start fails");
    assert!(
        error.contains(&format!("subscription {name} may not exist"))
            && error.ends_with(
                "the subscription check was refused: User not authorized to perform this action."
            ),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_notification_that_comes_while_the_start_waits_is_received_first() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let (_container, host) = emulator().await;
    let client = client(&host).await;
    let mut publisher = client.topic("gcs-events").new_publisher(None);
    let published = publisher
        .publish(PubsubMessage {
            data: b"{}".to_vec(),
            ..PubsubMessage::default()
        })
        .await
        .get()
        .await
        .expect("published");
    publisher.shutdown().await;

    let (name, stream) = subscribe(&client, "gcs-sub").await;
    let Ok(mut pull) = Pull::start(stream, refused(&name)).await else {
        panic!("the start of an existing subscription fails");
    };
    assert!(
        pull.first.is_some() && pull.unconfirmed.is_none(),
        "the start took the notification, which confirms the subscription"
    );
    let received = pull
        .receive(10, Duration::from_secs(1))
        .await
        .expect("a receive");
    let ids: Vec<&str> = received
        .iter()
        .map(|m| m.message.message_id.as_str())
        .collect();
    assert_eq!(ids, [published.as_str()]);
    for message in received {
        message.ack().await.expect("acknowledged");
    }
    pull.dispose().await;
}
