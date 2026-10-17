//! Google Cloud Storage plugin (`dev.mcpg.storage.gcs`): the `gcs_objects`
//! watch strategy.
//!
//! Each object created, modified or deleted under a bucket prefix is the
//! shared `object.changed` event. In `list` mode (the default) the watch
//! lists the prefix every `interval_ms` through the SDK's inventory helper,
//! which diffs it against the listing the gateway has stored and resumes
//! exactly after a restart; a Pub/Sub subscription to the bucket's
//! notifications, when given, makes it list at once. In `notifications`
//! mode that subscription alone is the source, for a bucket too large to
//! list: each notification is one event, acknowledged once the gateway has
//! stored it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use futures_util::StreamExt;
use gcloud_gax::conn::Environment;
use gcloud_gax::grpc::{Code, Status};
use gcloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile as PubsubCredentials;
use gcloud_pubsub::subscriber::{ReceivedMessage, SubscriberConfig};
use gcloud_pubsub::subscription::{MessageStream, SubscribeConfig};
use gcloud_storage::client::google_cloud_auth::credentials::CredentialsFile as StorageCredentials;
use gcloud_storage::http::objects::list::ListObjectsRequest;
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::inventory::{
    self, InventoryOptions, InventoryStart, OBJECT_CHANGED_EVENT_TYPE, ObjectEntry, Pattern,
    acknowledge_inventory_watch, cancel_inventory_watch, matches_key,
    spawn_inventory_watch_with_waker,
};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch,
    spawn_queue_watch,
};
use serde::Deserialize;
use serde_json::{Value, json};

pub const PLUGIN_ID: &str = "dev.mcpg.storage.gcs";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "gcs_objects";

/// The `store` of every event.
const STORE: &str = "gcs";

/// How long one receive of notifications waits for a message.
const RECEIVE_WAIT: Duration = Duration::from_secs(1);

/// How long the start watches an unchecked subscription's new stream for an
/// end: StreamingPull ends the stream of a missing subscription at once.
const CONFIRM_WAIT: Duration = Duration::from_secs(2);

/// How much longer than `ack_timeout_ms` the stream holds a notification.
const DEADLINE_MARGIN_SECS: u64 = 30;

/// Pub/Sub's bounds on a stream's ack deadline.
const MIN_DEADLINE_SECS: u64 = 10;
const MAX_DEADLINE_SECS: u64 = 600;

fn default_true() -> bool {
    true
}

fn default_interval_ms() -> u64 {
    60_000
}

fn default_timeout_ms() -> u64 {
    30_000
}

/// How the watch learns of changes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    /// List the prefix; notifications, when given, only wake the listing.
    #[default]
    List,
    /// Read only the notification subscription.
    Notifications,
}

/// The Pub/Sub subscription to the bucket's notification topic.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotificationsSpec {
    /// Its id, or `projects/{project}/subscriptions/{id}`.
    subscription: String,
    /// Default: the watch's `project`, or the one in a full name.
    #[serde(default)]
    project: Option<String>,
    /// A Pub/Sub emulator, `host:port`.
    #[serde(default)]
    emulator_host: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    bucket: String,
    /// The folder watched; a `/` is added when missing. Empty = the bucket.
    #[serde(default)]
    prefix: String,
    /// The project of the credentials and of a bare subscription id.
    #[serde(default)]
    project: Option<String>,
    /// A Google credentials file, as JSON: a `service_account` key, an
    /// `authorized_user` file or an `external_account` configuration;
    /// without it Application Default Credentials apply.
    #[serde(default)]
    credentials_json: Option<String>,
    /// Another Cloud Storage endpoint (`http://localhost:4443` for
    /// fake-gcs-server).
    #[serde(default)]
    endpoint_url: Option<String>,
    /// Send no credentials to Cloud Storage: a public bucket or an emulator.
    #[serde(default)]
    anonymous: bool,
    #[serde(default)]
    mode: Mode,
    /// Objects in "subfolders" of the prefix too.
    #[serde(default = "default_true")]
    recursive: bool,
    /// Keep only the objects whose name relative to the prefix matches.
    #[serde(default)]
    pattern: Option<String>,
    #[serde(default)]
    notifications: Option<NotificationsSpec>,
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "inventory::defaults::max_files")]
    max_files: usize,
    #[serde(default)]
    start_at: InventoryStart,
    #[serde(default = "inventory::defaults::snapshot_bytes")]
    snapshot_bytes: usize,
    #[serde(default = "inventory::defaults::max_in_flight")]
    max_in_flight: usize,
    #[serde(default = "inventory::defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
}

/// The project of a full subscription name.
fn project_of(subscription: &str) -> Option<&str> {
    let rest = subscription.strip_prefix("projects/")?;
    let (project, rest) = rest.split_once('/')?;
    rest.starts_with("subscriptions/").then_some(project)
}

fn millis(t: time::OffsetDateTime) -> Option<i64> {
    i64::try_from(t.unix_timestamp_nanos() / 1_000_000).ok()
}

/// The Cloud Storage client configuration, before authentication.
fn storage_config(spec: &WatchSpec) -> gcloud_storage::client::ClientConfig {
    let mut config = gcloud_storage::client::ClientConfig {
        project_id: spec.project.clone(),
        ..gcloud_storage::client::ClientConfig::default()
    };
    if let Some(endpoint) = &spec.endpoint_url {
        config.storage_endpoint = endpoint.trim_end_matches('/').to_owned();
    }
    config
}

/// The endpoints an `external_account` file may name, by field. Its
/// `credential_source` is a file or URL the gateway reads, and these
/// endpoints receive what the gateway read.
const EXTERNAL_ACCOUNT_ENDPOINTS: [(&str, &str); 3] = [
    ("token_url", "https://sts.googleapis.com/"),
    ("token_info_url", "https://sts.googleapis.com/"),
    (
        "service_account_impersonation_url",
        "https://iamcredentials.googleapis.com/",
    ),
];

/// Checks a `credentials_json` before it is used, and returns its
/// `project_id`.
fn check_credentials(json: &str) -> Result<Option<String>, String> {
    let file: Value = serde_json::from_str(json)
        .map_err(|e| format!("credentials_json is not a credentials file: {e}"))?;
    if file.get("type").and_then(Value::as_str) == Some("external_account") {
        for (field, base) in EXTERNAL_ACCOUNT_ENDPOINTS {
            match file.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::String(url)) if url.starts_with(base) => {}
                Some(_) => {
                    return Err(format!(
                        "credentials_json: the {field} of an external_account must start with {base}"
                    ));
                }
            }
        }
    }
    Ok(file
        .get("project_id")
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty())
        .map(str::to_owned))
}

/// `config` with `credentials_json`, or with Application Default
/// Credentials.
async fn storage_auth(
    config: gcloud_storage::client::ClientConfig,
    credentials_json: Option<&str>,
) -> Result<gcloud_storage::client::ClientConfig, String> {
    let credentials = match credentials_json {
        Some(json) => {
            check_credentials(json)?;
            let file = StorageCredentials::new_from_str(json)
                .await
                .map_err(|e| format!("credentials_json is not a credentials file: {e}"))?;
            Some(file)
        }
        None => None,
    };
    storage_with(config, credentials).await
}

/// `config` with `credentials`, or with Application Default Credentials.
async fn storage_with(
    config: gcloud_storage::client::ClientConfig,
    credentials: Option<StorageCredentials>,
) -> Result<gcloud_storage::client::ClientConfig, String> {
    match credentials {
        Some(credentials) => config.with_credentials(credentials).await,
        None => config.with_auth().await,
    }
    .map_err(|e| format!("Cloud Storage authentication failed: {e}"))
}

/// The objects of one listing, keyed relative to `prefix`; the first
/// `max_files` in name order when there are more.
async fn list_objects(
    client: &gcloud_storage::client::Client,
    bucket: &str,
    prefix: &str,
    recursive: bool,
    pattern: Option<&Pattern>,
    max_files: usize,
) -> Result<Vec<ObjectEntry>, String> {
    let mut entries = Vec::new();
    let mut page_token = None;
    loop {
        let request = ListObjectsRequest {
            bucket: bucket.to_owned(),
            prefix: Some(prefix.to_owned()).filter(|p| !p.is_empty()),
            delimiter: (!recursive).then(|| "/".to_owned()),
            page_token: page_token.take(),
            ..ListObjectsRequest::default()
        };
        let page = client
            .list_objects(&request)
            .await
            .map_err(|e| format!("Cloud Storage list failed: {e}"))?;
        for object in page.items.unwrap_or_default() {
            let relative = object.name.strip_prefix(prefix).unwrap_or(&object.name);
            if relative.is_empty() || relative.ends_with('/') {
                continue;
            }
            if pattern.is_some_and(|p| !matches_key(p, relative)) {
                continue;
            }
            // Cloud Storage lists in name order, so the first `max_files`
            // are final.
            if entries.len() == max_files {
                tracing::warn!(
                    bucket,
                    prefix,
                    max_files,
                    "gcs_objects: more objects than max_files; the listing is cut"
                );
                return Ok(entries);
            }
            entries.push(ObjectEntry {
                key: relative.to_owned(),
                size: u64::try_from(object.size).ok(),
                modified_at_ms: object.updated.and_then(millis),
                etag: Some(object.etag.clone()).filter(|e| !e.is_empty()),
                version: Some(object.generation.to_string()),
            });
        }
        match page.next_page_token {
            Some(token) if !token.is_empty() => page_token = Some(token),
            _ => return Ok(entries),
        }
    }
}

/// What a Cloud Storage notification says happened.
#[derive(Debug, Clone, PartialEq)]
struct Notice {
    bucket: String,
    key: String,
    op: &'static str,
    generation: Option<String>,
    size: Option<u64>,
    etag: Option<String>,
    updated: Option<String>,
    event_type: String,
    event_time: Option<String>,
}

/// The object change of one notification, from its attributes and its
/// `JSON_API_V1` payload. A metadata update, and the delete or archive of a
/// generation another write replaced, are no object change: the write
/// reports itself.
fn parse_notice(attributes: &HashMap<String, String>, data: &[u8]) -> Option<Notice> {
    let event_type = attributes.get("eventType")?.clone();
    let op = match event_type.as_str() {
        "OBJECT_FINALIZE" if attributes.contains_key("overwroteGeneration") => "modified",
        "OBJECT_FINALIZE" => "created",
        "OBJECT_DELETE" | "OBJECT_ARCHIVE"
            if !attributes.contains_key("overwrittenByGeneration") =>
        {
            "deleted"
        }
        _ => return None,
    };
    let payload: Value = serde_json::from_slice(data).unwrap_or(Value::Null);
    let text = |k: &str| payload.get(k).and_then(Value::as_str).map(str::to_owned);
    Some(Notice {
        bucket: attributes.get("bucketId")?.clone(),
        key: attributes.get("objectId")?.clone(),
        op,
        generation: attributes.get("objectGeneration").cloned(),
        // The JSON API carries a size as a string.
        size: payload.get("size").and_then(|s| {
            s.as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| s.as_u64())
        }),
        etag: text("etag"),
        updated: text("updated"),
        event_type,
        event_time: attributes.get("eventTime").cloned(),
    })
}

/// Which notifications a watch reports, and the key each is reported under.
struct Scope {
    bucket: String,
    prefix: String,
    recursive: bool,
    pattern: Option<Pattern>,
    location: String,
}

impl Scope {
    fn relative<'a>(&self, notice: &'a Notice) -> Option<&'a str> {
        if notice.bucket != self.bucket {
            return None;
        }
        let relative = notice.key.strip_prefix(&self.prefix)?;
        if relative.is_empty() || relative.ends_with('/') {
            return None;
        }
        if !self.recursive && relative.contains('/') {
            return None;
        }
        if self
            .pattern
            .as_ref()
            .is_some_and(|p| !matches_key(p, relative))
        {
            return None;
        }
        Some(relative)
    }

    /// The `object.changed` event of one notice, its id kept across
    /// redeliveries.
    fn event(&self, notice: &Notice, key: &str) -> WatchEvent {
        let mut hasher = blake3::Hasher::new();
        let mark = notice.generation.as_deref().unwrap_or("");
        for part in [STORE, self.location.as_str(), key, notice.op, mark] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        let mut data = json!({
            "op": notice.op,
            "store": STORE,
            "location": self.location,
            "key": key,
            "eventName": notice.event_type,
        });
        let alive = notice.op != "deleted";
        if let Some(size) = notice.size.filter(|_| alive) {
            data["size"] = json!(size);
        }
        if let Some(etag) = &notice.etag {
            data["etag"] = json!(etag);
        }
        if let Some(generation) = &notice.generation {
            data["version"] = json!(generation);
        }
        let modified_at = notice.updated.clone().or_else(|| notice.event_time.clone());
        if alive && let Some(at) = &modified_at {
            data["modifiedAt"] = json!(at);
        }
        WatchEvent {
            event_id: Some(format!("obj-{}", &hasher.finalize().to_hex()[..32])),
            event_type: Some(OBJECT_CHANGED_EVENT_TYPE.to_owned()),
            occurred_at: notice.event_time.clone().or(modified_at),
            data: Some(data),
            ..WatchEvent::default()
        }
    }
}

/// How to reach the notification subscription.
#[derive(Clone)]
struct Subscription {
    name: String,
    project: Option<String>,
    credentials_json: Option<String>,
    emulator_host: Option<String>,
    deadline_secs: i32,
    max_outstanding: i64,
}

/// A subscription the start subscribes to although its check was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Unconfirmed {
    name: String,
    /// The status message of the refused check.
    refusal: String,
}

/// What the start does after it checks that the subscription exists:
/// `Some` when it subscribes without the check. The check needs
/// `pubsub.subscriptions.get`, which `roles/pubsub.subscriber` does not
/// have, so a refused check does not stop the start.
fn after_check(name: &str, checked: Result<bool, Status>) -> Result<Option<Unconfirmed>, String> {
    match checked {
        Ok(true) => Ok(None),
        Err(status) if status.code() == Code::PermissionDenied => {
            tracing::debug!(
                subscription = %name,
                reason = %status.message(),
                "gcs_objects: the subscription check is refused; subscribing"
            );
            Ok(Some(Unconfirmed {
                name: name.to_owned(),
                refusal: status.message().to_owned(),
            }))
        }
        Ok(false) => Err(format!("Pub/Sub subscription {name} does not exist")),
        Err(status) if status.code() == Code::NotFound => {
            Err(format!("Pub/Sub subscription {name} does not exist"))
        }
        Err(status) => Err(format!(
            "Pub/Sub subscription check failed: {}",
            status.message()
        )),
    }
}

/// The stream's settings. Without its own subscriber settings, `subscribe`
/// reads the subscription's, which needs `pubsub.subscriptions.get`.
fn subscribe_config(deadline_secs: i32, max_outstanding: i64) -> SubscribeConfig {
    SubscribeConfig::default().with_subscriber_config(SubscriberConfig {
        stream_ack_deadline_seconds: deadline_secs,
        max_outstanding_messages: max_outstanding,
        ..SubscriberConfig::default()
    })
}

/// The error of a stream that ended.
fn stream_ended(unconfirmed: Option<&Unconfirmed>) -> String {
    match unconfirmed {
        Some(Unconfirmed { name, refusal }) => format!(
            "the Pub/Sub stream ended before its first message: subscription {name} may not \
             exist, or the watcher may lack pubsub.subscriptions.consume on it; the \
             subscription check was refused: {refusal}"
        ),
        None => "the Pub/Sub stream ended".to_owned(),
    }
}

/// An open StreamingPull.
struct Pull {
    stream: MessageStream,
    /// A message taken while the start watched the stream.
    first: Option<ReceivedMessage>,
    /// Set while the subscription's check was refused and no message has
    /// come.
    unconfirmed: Option<Unconfirmed>,
}

impl Pull {
    /// The pull of a new stream. The stream of an unchecked subscription is
    /// watched for `CONFIRM_WAIT` first, so that a missing subscription
    /// fails the start.
    async fn start(
        stream: MessageStream,
        unconfirmed: Option<Unconfirmed>,
    ) -> Result<Self, String> {
        let mut pull = Self {
            stream,
            first: None,
            unconfirmed,
        };
        if pull.unconfirmed.is_some() {
            match tokio::time::timeout(CONFIRM_WAIT, pull.stream.next()).await {
                Err(_) => {}
                Ok(Some(message)) => {
                    pull.first = Some(message);
                    pull.unconfirmed = None;
                }
                Ok(None) => {
                    let ended = stream_ended(pull.unconfirmed.as_ref());
                    pull.dispose().await;
                    return Err(ended);
                }
            }
        }
        Ok(pull)
    }

    /// Up to `max` messages, waiting at most `wait` for the first.
    async fn receive(
        &mut self,
        max: usize,
        wait: Duration,
    ) -> Result<Vec<ReceivedMessage>, String> {
        let mut received: Vec<ReceivedMessage> = self.first.take().into_iter().collect();
        if received.is_empty() {
            match tokio::time::timeout(wait, self.stream.next()).await {
                Err(_) => return Ok(received),
                Ok(None) => return Err(stream_ended(self.unconfirmed.as_ref())),
                Ok(Some(message)) => received.push(message),
            }
        }
        self.unconfirmed = None;
        while received.len() < max {
            match tokio::time::timeout(Duration::ZERO, self.stream.next()).await {
                Ok(Some(message)) => received.push(message),
                _ => break,
            }
        }
        Ok(received)
    }

    /// Nacks what the pull holds and closes the stream.
    async fn dispose(self) {
        if let Some(message) = self.first {
            let _ = message.nack().await;
        }
        self.stream.dispose().await;
    }
}

impl Subscription {
    /// The Pub/Sub client configuration: the emulator, or Google Cloud with
    /// `credentials_json` or Application Default Credentials.
    async fn config(&self) -> Result<gcloud_pubsub::client::ClientConfig, String> {
        use gcloud_pubsub::client::ClientConfig;
        let config = ClientConfig {
            project_id: self.project.clone(),
            ..ClientConfig::default()
        };
        if let Some(host) = &self.emulator_host {
            return Ok(ClientConfig {
                environment: Environment::Emulator(host.clone()),
                project_id: config.project_id.or_else(|| Some("local-project".into())),
                ..config
            });
        }
        let credentials = match &self.credentials_json {
            Some(json) => {
                check_credentials(json)?;
                let file = PubsubCredentials::new_from_str(json)
                    .await
                    .map_err(|e| format!("credentials_json is not a credentials file: {e}"))?;
                Some(file)
            }
            None => None,
        };
        pubsub_with(config, credentials).await
    }

    async fn open(&self) -> Result<Pull, String> {
        let client = gcloud_pubsub::client::Client::new(self.config().await?)
            .await
            .map_err(|e| format!("Pub/Sub client failed: {e}"))?;
        let subscription = client.subscription(&self.name);
        let unconfirmed = after_check(
            subscription.fully_qualified_name(),
            subscription.exists(None).await,
        )?;
        let stream = subscription
            .subscribe(Some(subscribe_config(
                self.deadline_secs,
                self.max_outstanding,
            )))
            .await
            .map_err(|e| format!("Pub/Sub subscribe failed: {}", e.message()))?;
        Pull::start(stream, unconfirmed).await
    }
}

/// Google Cloud Pub/Sub with `credentials`, or with Application Default
/// Credentials.
async fn pubsub_with(
    config: gcloud_pubsub::client::ClientConfig,
    credentials: Option<PubsubCredentials>,
) -> Result<gcloud_pubsub::client::ClientConfig, String> {
    // Never an emulator a stray PUBSUB_EMULATOR_HOST names.
    let config = gcloud_pubsub::client::ClientConfig {
        environment: Environment::GoogleCloud(Box::new(token_source::NoopTokenSourceProvider {})),
        ..config
    };
    match credentials {
        Some(credentials) => config.with_credentials(credentials).await,
        None => config.with_auth().await,
    }
    .map_err(|e| format!("Pub/Sub authentication failed: {e}"))
}

/// `notifications` mode: the subscription as a queue source, each relevant
/// notification one delivery.
struct NotificationSource {
    subscription: Subscription,
    scope: Scope,
    rt: tokio::runtime::Runtime,
    pull: Mutex<Option<Pull>>,
    held: Mutex<HashMap<u64, ReceivedMessage>>,
    next_receipt: AtomicU64,
}

impl NotificationSource {
    fn held(&self, receipt: &Value) -> Option<ReceivedMessage> {
        let receipt = receipt.as_u64()?;
        self.held
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&receipt)
    }
}

impl QueueSource for NotificationSource {
    fn connect(&self) -> Result<(), String> {
        let mut pull = self.pull.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = pull.take() {
            self.rt.block_on(old.dispose());
        }
        *pull = Some(self.rt.block_on(self.subscription.open())?);
        Ok(())
    }

    fn receive(&self, max: usize, wait: Duration) -> Result<Vec<Delivery>, String> {
        let mut pull = self.pull.lock().unwrap_or_else(|e| e.into_inner());
        let Some(pull) = pull.as_mut() else {
            return Err("not connected".into());
        };
        let received = self.rt.block_on(pull.receive(max, wait))?;
        let mut deliveries = Vec::new();
        for message in received {
            let notice = parse_notice(&message.message.attributes, &message.message.data);
            let relevant = notice.and_then(|notice| {
                self.scope
                    .relative(&notice)
                    .map(|key| self.scope.event(&notice, key))
            });
            let Some(event) = relevant else {
                // A metadata update, another bucket, or a key out of scope.
                if let Err(error) = self.rt.block_on(message.ack()) {
                    tracing::warn!(error = %error.message(), "gcs_objects: cannot acknowledge a notification of nothing watched");
                }
                continue;
            };
            let receipt = self.next_receipt.fetch_add(1, Ordering::Relaxed);
            self.held
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(receipt, message);
            deliveries.push(Delivery {
                event,
                receipt: Value::from(receipt),
            });
        }
        Ok(deliveries)
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let Some(message) = self.held(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(message.ack())
            .map_err(|e| format!("Pub/Sub ack failed: {}", e.message()))
    }

    fn release(&self, receipt: &Value) -> Result<(), String> {
        let Some(message) = self.held(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(message.nack())
            .map_err(|e| format!("Pub/Sub nack failed: {}", e.message()))
    }
}

impl Drop for NotificationSource {
    fn drop(&mut self) {
        let pull = self.pull.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(pull) = pull.take() {
            self.rt.block_on(pull.dispose());
        }
    }
}

/// A `list` mode watch, with the thread that turns notifications into
/// early listings when it has one.
struct ListWatch {
    inventory: WatchHandleBox,
    waker: Option<(Arc<AtomicBool>, JoinHandle<()>)>,
}

enum WatchState {
    List(ListWatch),
    Notifications(WatchHandleBox),
}

fn boxed(state: WatchState) -> WatchHandleBox {
    WatchHandleBox(Box::into_raw(Box::new(state)) as *mut ())
}

/// Wake `waker` for each batch of notifications on `subscription`, and
/// acknowledge them: the listing is what reports, so a notification is a
/// hint only.
fn wake_on_notifications(
    rt: Arc<tokio::runtime::Runtime>,
    subscription: Subscription,
    waker: inventory::InventoryWaker,
    stop: Arc<AtomicBool>,
) {
    let pause = |backoff: Duration| {
        let until = std::time::Instant::now() + backoff;
        while std::time::Instant::now() < until && !stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    let mut backoff = Duration::from_millis(500);
    let mut pull: Option<Pull> = None;
    while !stop.load(Ordering::Acquire) {
        let Some(open) = pull.as_mut() else {
            match rt.block_on(subscription.open()) {
                Ok(opened) => pull = Some(opened),
                Err(error) => {
                    tracing::warn!(error = %error, "gcs_objects: notifications unavailable; the listing goes on at its interval");
                    pause(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
            continue;
        };
        match rt.block_on(open.receive(100, RECEIVE_WAIT)) {
            Ok(messages) => {
                // A stream that stays open resets the backoff, not one that
                // only opens: an unchecked subscription opens and then ends.
                backoff = Duration::from_millis(500);
                if !messages.is_empty() {
                    waker.wake();
                    for message in messages {
                        let _ = rt.block_on(message.ack());
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "gcs_objects: the notification stream ended; opening it again");
                if let Some(old) = pull.take() {
                    rt.block_on(old.dispose());
                }
                pause(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
    if let Some(pull) = pull.take() {
        rt.block_on(pull.dispose());
    }
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// bucket and credentials arrive on the per-watch spec.
pub struct GcsWatchPlugin {
    manifest: PluginManifest,
}

impl GcsWatchPlugin {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.storage.gcs",
                name: "Cloud Storage Objects Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for GcsWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, resume) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid gcs_objects watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if parsed.bucket.trim().is_empty() {
            return Err(invalid("bucket must not be empty".into()));
        }
        if parsed
            .credentials_json
            .as_deref()
            .is_some_and(|c| c.starts_with("cred://"))
        {
            return Err(invalid(
                "credentials_json must not be a cred:// URI — per-caller credentials are \
                 unsupported; use ${secret.X} / vault:// (resolved before the watch starts)"
                    .into(),
            ));
        }
        if parsed.anonymous && parsed.credentials_json.is_some() {
            return Err(invalid(
                "anonymous and credentials_json do not go together".into(),
            ));
        }
        let credentials_project = match parsed.credentials_json.as_deref() {
            Some(json) => check_credentials(json).map_err(invalid)?,
            None => None,
        };
        if !(1..=100_000).contains(&parsed.max_files) {
            return Err(invalid("max_files must be 1 to 100000".into()));
        }
        if parsed.timeout_ms == 0 {
            return Err(invalid("timeout_ms must be greater than 0".into()));
        }
        if parsed.mode == Mode::Notifications && parsed.notifications.is_none() {
            return Err(invalid(
                "mode notifications needs notifications.subscription".into(),
            ));
        }
        if let Some(notifications) = &parsed.notifications {
            let name = notifications.subscription.trim();
            if name.is_empty() || (name.contains('/') && project_of(name).is_none()) {
                return Err(invalid(
                    "notifications.subscription must be an id or \
                     projects/{project}/subscriptions/{id}"
                        .into(),
                ));
            }
            // The client takes a project from a credentials file's
            // project_id only, so without one every connect would fail.
            let projectless = notifications.project.is_none()
                && project_of(name).is_none()
                && parsed.project.is_none()
                && notifications.emulator_host.is_none();
            if projectless && parsed.credentials_json.is_some() && credentials_project.is_none() {
                return Err(invalid(
                    "a bare notifications.subscription id needs notifications.project or \
                     project when credentials_json has no project_id"
                        .into(),
                ));
            }
        }
        let deadline_secs =
            (parsed.ack_timeout_ms.div_ceil(1_000) + DEADLINE_MARGIN_SECS).max(MIN_DEADLINE_SECS);
        if deadline_secs > MAX_DEADLINE_SECS {
            return Err(invalid(format!(
                "ack_timeout_ms must be at most {}",
                (MAX_DEADLINE_SECS - DEADLINE_MARGIN_SECS) * 1_000
            )));
        }
        let pattern = parsed
            .pattern
            .as_deref()
            .map(Pattern::new)
            .transpose()
            .map_err(|e| invalid(format!("pattern: {e}")))?;
        let prefix = if parsed.prefix.is_empty() || parsed.prefix.ends_with('/') {
            parsed.prefix.clone()
        } else {
            format!("{}/", parsed.prefix)
        };
        let location = format!("gs://{}/{prefix}", parsed.bucket);
        let subscription = parsed.notifications.as_ref().map(|n| {
            let name = n.subscription.trim().to_owned();
            Subscription {
                project: n
                    .project
                    .clone()
                    .or_else(|| project_of(&name).map(str::to_owned))
                    .or_else(|| parsed.project.clone()),
                name,
                credentials_json: parsed.credentials_json.clone(),
                emulator_host: n.emulator_host.clone(),
                deadline_secs: i32::try_from(deadline_secs).unwrap_or(600),
                max_outstanding: i64::try_from(parsed.max_in_flight).unwrap_or(1000),
            }
        });
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mcpg-gcs-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("gcs_objects: tokio runtime init failed: {e}"),
            })?;

        if parsed.mode == Mode::Notifications {
            let subscription = subscription
                .ok_or_else(|| invalid("notifications.subscription is missing".into()))?;
            let options = QueueOptions {
                source: format!("gcs notifications {}", subscription.name),
                max_in_flight: parsed.max_in_flight,
                ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
                receive_wait: RECEIVE_WAIT,
            };
            options.validate().map_err(invalid)?;
            let source = NotificationSource {
                subscription,
                scope: Scope {
                    bucket: parsed.bucket,
                    prefix,
                    recursive: parsed.recursive,
                    pattern,
                    location,
                },
                rt,
                pull: Mutex::new(None),
                held: Mutex::new(HashMap::new()),
                next_receipt: AtomicU64::new(0),
            };
            let handle = spawn_queue_watch(resource_uri, options, emit_event, source)?;
            return Ok(boxed(WatchState::Notifications(handle)));
        }

        let config = storage_config(&parsed);
        let config = if parsed.anonymous {
            config.anonymous()
        } else {
            rt.block_on(storage_auth(config, parsed.credentials_json.as_deref()))
                .map_err(|message| WatchError::Subscribe { message })?
        };
        let client = gcloud_storage::client::Client::new(config);
        let rt = Arc::new(rt);
        let listing_rt = Arc::clone(&rt);
        let timeout = Duration::from_millis(parsed.timeout_ms);
        let (bucket, recursive, max_files) =
            (parsed.bucket.clone(), parsed.recursive, parsed.max_files);
        let listing_prefix = prefix.clone();
        // A bucket that cannot be listed now is listed again at the next
        // interval: the source is not lost to a transient outage.
        let list = move || {
            listing_rt.block_on(async {
                tokio::time::timeout(
                    timeout,
                    list_objects(
                        &client,
                        &bucket,
                        &listing_prefix,
                        recursive,
                        pattern.as_ref(),
                        max_files,
                    ),
                )
                .await
                .map_err(|_| "Cloud Storage listing timed out".to_owned())?
            })
        };
        let (inventory, waker) = spawn_inventory_watch_with_waker(
            resource_uri,
            InventoryOptions {
                store: STORE.into(),
                location,
                interval: Duration::from_millis(parsed.interval_ms),
                start: parsed.start_at,
                snapshot_bytes: parsed.snapshot_bytes,
                max_in_flight: parsed.max_in_flight,
                ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            },
            resume,
            emit_event,
            list,
        )?;
        let waker = match subscription {
            None => None,
            Some(subscription) => {
                let stop = Arc::new(AtomicBool::new(false));
                let stopping = Arc::clone(&stop);
                let spawned = std::thread::Builder::new()
                    .name("mcpg-gcs-notify".to_owned())
                    .spawn(move || wake_on_notifications(rt, subscription, waker, stopping));
                match spawned {
                    Ok(join) => Some((stop, join)),
                    Err(e) => {
                        cancel_inventory_watch(inventory);
                        return Err(WatchError::Subscribe {
                            message: format!(
                                "gcs_objects: cannot start the notification thread: {e}"
                            ),
                        });
                    }
                }
            }
        };
        Ok(boxed(WatchState::List(ListWatch { inventory, waker })))
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: the pointer `watch` returned, still owned by the host,
        // which never acknowledges after cancel.
        #[allow(unsafe_code)]
        let state = unsafe { &*(watch_handle.0 as *const WatchState) };
        match state {
            WatchState::List(list) => acknowledge_inventory_watch(&list.inventory, position),
            WatchState::Notifications(handle) => acknowledge_queue_watch(handle, position),
        }
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: produced by `Box::into_raw` in `boxed` and round-tripped by
        // the host exactly once.
        #[allow(unsafe_code)]
        let state = unsafe { Box::from_raw(watch_handle.0 as *mut WatchState) };
        match *state {
            WatchState::List(list) => {
                if let Some((stop, join)) = list.waker {
                    stop.store(true, Ordering::Release);
                    let _ = join.join();
                }
                cancel_inventory_watch(list.inventory);
            }
            WatchState::Notifications(handle) => cancel_queue_watch(handle),
        }
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.storage.gcs",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: GcsWatchPlugin,
            factory: |cfg: &str, host: ::mcpg_plugin_sdk::HostHandle|
                GcsWatchPlugin::from_host_config(cfg, host),
        },
    ],
}

#[cfg(test)]
#[path = "watch_tests.rs"]
mod tests;

#[cfg(all(test, feature = "integration-tests"))]
#[path = "emulator_tests.rs"]
mod emulator_tests;
