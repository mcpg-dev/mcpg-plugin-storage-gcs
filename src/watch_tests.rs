use super::*;

fn stub_host() -> HostHandle {
    // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
    // factory ignores the host entirely.
    #[allow(unsafe_code)]
    unsafe {
        HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
    }
}

fn plugin() -> GcsWatchPlugin {
    GcsWatchPlugin::from_host_config("", stub_host())
}

fn refused(spec: Value) -> String {
    match plugin().watch("mcpg-event://drops", &spec, Box::new(|_| {})) {
        Err(WatchError::InvalidSpec { message }) => message,
        Err(other) => panic!("not a spec error: {other:?}"),
        Ok(handle) => {
            plugin().cancel(handle);
            panic!("accepted {spec}");
        }
    }
}

fn attributes(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn notice(key: &str, op: &'static str) -> Notice {
    Notice {
        bucket: "drops".into(),
        key: key.into(),
        op,
        generation: Some("1700000000000001".into()),
        size: Some(5),
        etag: Some("CJ".into()),
        updated: Some("2026-10-10T12:00:00.000Z".into()),
        event_type: "OBJECT_FINALIZE".into(),
        event_time: Some("2026-10-10T12:00:00.100Z".into()),
    }
}

fn scope(prefix: &str, recursive: bool, pattern: Option<&str>) -> Scope {
    Scope {
        bucket: "drops".into(),
        prefix: prefix.into(),
        recursive,
        pattern: pattern.map(|p| Pattern::new(p).unwrap()),
        location: format!("gs://drops/{prefix}"),
    }
}

#[test]
fn manifest_and_kind_are_correct() {
    use mcpg_plugin_protocol::PluginClass;
    let p = plugin();
    assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
    assert_eq!(
        SyncWatchStrategyPlugin::manifest(&p).plugin_class,
        PluginClass::WatchStrategy
    );
    assert_eq!(p.kind(), WATCH_KIND);
}

#[test]
fn spec_parses_with_defaults() {
    let parsed: WatchSpec = serde_json::from_value(json!({ "bucket": "drops" })).unwrap();
    assert_eq!(parsed.mode, Mode::List);
    assert!(parsed.recursive && !parsed.anonymous);
    assert_eq!(parsed.interval_ms, 60_000);
    assert_eq!(parsed.timeout_ms, 30_000);
    assert_eq!(parsed.max_files, 10_000);
    assert_eq!(parsed.start_at, InventoryStart::Now);
}

#[test]
fn a_bad_spec_is_refused_before_anything_is_called() {
    let with = |extra: Value| {
        let mut spec = json!({ "bucket": "drops" });
        for (k, v) in extra.as_object().unwrap() {
            spec[k] = v.clone();
        }
        spec
    };
    assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
    assert!(refused(with(json!({ "bucket": "" }))).contains("bucket"));
    assert!(refused(with(json!({ "credentials_json": "cred://vault/gcp" }))).contains("cred://"));
    assert!(
        refused(with(json!({ "anonymous": true, "credentials_json": "{}" })))
            .contains("do not go together")
    );
    assert!(refused(with(json!({ "pattern": "[a" }))).contains("pattern"));
    assert!(refused(with(json!({ "max_files": 0 }))).contains("max_files"));
    assert!(refused(with(json!({ "timeout_ms": 0 }))).contains("timeout_ms"));
    assert!(refused(with(json!({ "mode": "notifications" }))).contains("notifications"));
    assert!(
        refused(with(
            json!({ "notifications": { "subscription": "projects/p/topics/t" } })
        ))
        .contains("subscription")
    );
    assert!(refused(with(json!({ "ack_timeout_ms": 580_000 }))).contains("ack_timeout_ms"));
    assert!(refused(with(json!({ "anonymous": true, "interval_ms": 10 }))).contains("interval_ms"));
}

#[test]
fn a_notification_says_created_modified_or_deleted() {
    let data = json!({ "name": "in/a.csv", "size": "7", "etag": "CJ", "updated": "2026-10-10T12:00:00.000Z" })
        .to_string();
    let finalize = attributes(&[
        ("eventType", "OBJECT_FINALIZE"),
        ("bucketId", "drops"),
        ("objectId", "in/a.csv"),
        ("objectGeneration", "2"),
        ("eventTime", "2026-10-10T12:00:00.100Z"),
    ]);
    let created = parse_notice(&finalize, data.as_bytes()).unwrap();
    assert_eq!(created.op, "created");
    assert_eq!(created.size, Some(7), "the JSON API's string size");
    assert_eq!(created.generation.as_deref(), Some("2"));

    let mut overwrite = finalize.clone();
    overwrite.insert("overwroteGeneration".into(), "1".into());
    assert_eq!(
        parse_notice(&overwrite, data.as_bytes()).unwrap().op,
        "modified"
    );

    let delete = attributes(&[
        ("eventType", "OBJECT_DELETE"),
        ("bucketId", "drops"),
        ("objectId", "in/a.csv"),
    ]);
    assert_eq!(parse_notice(&delete, b"").unwrap().op, "deleted");
    let mut replaced = delete.clone();
    replaced.insert("overwrittenByGeneration".into(), "3".into());
    assert_eq!(
        parse_notice(&replaced, b""),
        None,
        "the write reports itself"
    );
    let archive = attributes(&[
        ("eventType", "OBJECT_ARCHIVE"),
        ("bucketId", "drops"),
        ("objectId", "in/a.csv"),
    ]);
    assert_eq!(parse_notice(&archive, b"").unwrap().op, "deleted");
    let metadata = attributes(&[
        ("eventType", "OBJECT_METADATA_UPDATE"),
        ("bucketId", "drops"),
        ("objectId", "in/a.csv"),
    ]);
    assert_eq!(parse_notice(&metadata, b""), None);
    assert_eq!(parse_notice(&HashMap::new(), b""), None);
}

#[test]
fn the_scope_keeps_its_bucket_prefix_depth_and_pattern() {
    let all = scope("in/", true, None);
    assert_eq!(all.relative(&notice("in/a.csv", "created")), Some("a.csv"));
    assert_eq!(
        all.relative(&notice("in/sub/b.csv", "created")),
        Some("sub/b.csv")
    );
    assert_eq!(all.relative(&notice("out/a.csv", "created")), None);
    let elsewhere = Notice {
        bucket: "other".into(),
        ..notice("in/a.csv", "created")
    };
    assert_eq!(all.relative(&elsewhere), None);
    assert_eq!(
        scope("in/", false, None).relative(&notice("in/sub/b.csv", "created")),
        None
    );
    let csv = scope("in/", true, Some("*.csv"));
    assert_eq!(csv.relative(&notice("in/sub/b.csv", "created")), None);
    assert_eq!(csv.relative(&notice("in/a.txt", "created")), None);
}

#[test]
fn a_notice_is_an_object_changed_event_with_a_stable_id() {
    let scope = scope("in/", true, None);
    let created = scope.event(&notice("in/a.csv", "created"), "a.csv");
    let data = created.data.as_ref().unwrap();
    assert_eq!(data["op"], json!("created"));
    assert_eq!(data["store"], json!("gcs"));
    assert_eq!(data["location"], json!("gs://drops/in/"));
    assert_eq!(data["key"], json!("a.csv"));
    assert_eq!(data["size"], json!(5));
    assert_eq!(data["version"], json!("1700000000000001"));
    assert_eq!(data["modifiedAt"], json!("2026-10-10T12:00:00.000Z"));
    assert_eq!(
        created.occurred_at.as_deref(),
        Some("2026-10-10T12:00:00.100Z")
    );
    assert_eq!(
        scope
            .event(&notice("in/a.csv", "created"), "a.csv")
            .event_id,
        created.event_id
    );
    let next = Notice {
        generation: Some("1700000000000002".into()),
        ..notice("in/a.csv", "modified")
    };
    assert_ne!(scope.event(&next, "a.csv").event_id, created.event_id);
    let deleted = scope.event(&notice("in/a.csv", "deleted"), "a.csv");
    let data = deleted.data.unwrap();
    assert!(data.get("size").is_none() && data.get("modifiedAt").is_none());
}

const NAME: &str = "projects/acme/subscriptions/drops";

#[test]
fn a_refused_subscription_check_does_not_stop_the_start() {
    assert_eq!(after_check(NAME, Ok(true)), Ok(None));
    assert_eq!(
        after_check(
            NAME,
            Err(Status::new(Code::PermissionDenied, "User not authorized"))
        ),
        Ok(Some(Unconfirmed {
            name: NAME.into(),
            refusal: "User not authorized".into(),
        }))
    );
    let missing = format!("Pub/Sub subscription {NAME} does not exist");
    assert_eq!(after_check(NAME, Ok(false)), Err(missing.clone()));
    assert_eq!(
        after_check(NAME, Err(Status::new(Code::NotFound, "gone"))),
        Err(missing)
    );
    for code in [
        Code::Unauthenticated,
        Code::Unavailable,
        Code::InvalidArgument,
        Code::Internal,
    ] {
        assert_eq!(
            after_check(NAME, Err(Status::new(code, "boom"))),
            Err("Pub/Sub subscription check failed: boom".into()),
            "{code:?}"
        );
    }
}

#[test]
fn an_unconfirmed_stream_that_ends_names_the_subscription_and_the_refusal() {
    let ended = stream_ended(Some(&Unconfirmed {
        name: NAME.into(),
        refusal: "Cloud Pub/Sub API has not been used in project acme".into(),
    }));
    assert!(
        ended.contains(&format!("subscription {NAME} may not exist"))
            && ended.contains("pubsub.subscriptions.consume")
            && ended.ends_with(
                "the subscription check was refused: Cloud Pub/Sub API has not been used in \
                 project acme"
            ),
        "{ended}"
    );
    assert_eq!(stream_ended(None), "the Pub/Sub stream ended");
}

#[test]
fn an_external_account_sends_its_tokens_to_google_only() {
    let google: Value = serde_json::from_str(&external_account(
        "https://sts.googleapis.com",
        "https://iamcredentials.googleapis.com",
        "http://127.0.0.1:1",
    ))
    .unwrap();
    assert_eq!(check_credentials(&google.to_string()), Ok(None));
    let mut with_info = google.clone();
    with_info["token_info_url"] = json!("https://sts.googleapis.com/v1/introspect");
    assert_eq!(check_credentials(&with_info.to_string()), Ok(None));

    for (field, url) in [
        ("token_url", "https://collector.example.com/v1/token"),
        ("token_url", "http://sts.googleapis.com/v1/token"),
        (
            "token_url",
            "https://sts.googleapis.com.example.com/v1/token",
        ),
        (
            "token_url",
            "https://sts.googleapis.com@example.com/v1/token",
        ),
        ("token_info_url", "https://collector.example.com/introspect"),
        (
            "service_account_impersonation_url",
            "https://collector.example.com/generateAccessToken",
        ),
    ] {
        let mut file = google.clone();
        file[field] = json!(url);
        let refused = check_credentials(&file.to_string()).expect_err(url);
        assert!(
            refused.contains(field) && refused.contains("must start with https://"),
            "{refused}"
        );
    }
    let mut not_text = google;
    not_text["token_url"] = json!(["https://sts.googleapis.com/v1/token"]);
    assert!(check_credentials(&not_text.to_string()).is_err());
}

#[test]
fn other_credentials_files_pass_with_their_project() {
    let key = json!({
        "type": "service_account",
        "project_id": "acme",
        "token_uri": "https://oauth2.googleapis.com/token",
    });
    assert_eq!(check_credentials(&key.to_string()), Ok(Some("acme".into())));
    let user = json!({ "type": "authorized_user", "client_id": "c", "refresh_token": "r" });
    assert_eq!(check_credentials(&user.to_string()), Ok(None));
    assert!(
        check_credentials("not json")
            .unwrap_err()
            .contains("not a credentials file")
    );
}

#[test]
fn credentials_are_checked_before_the_watch_starts() {
    let with = |extra: Value| {
        let mut spec = json!({ "bucket": "drops" });
        for (k, v) in extra.as_object().unwrap() {
            spec[k] = v.clone();
        }
        spec
    };
    let elsewhere = external_account(
        "http://127.0.0.1:1",
        "http://127.0.0.1:1",
        "http://127.0.0.1:1",
    );
    assert!(
        refused(with(json!({ "credentials_json": elsewhere })))
            .contains("must start with https://sts.googleapis.com/")
    );
    assert!(
        refused(with(json!({ "credentials_json": "not json" }))).contains("not a credentials file")
    );
    let user = json!({ "type": "authorized_user", "client_id": "c", "refresh_token": "r" });
    assert!(
        refused(with(json!({
            "credentials_json": user.to_string(),
            "mode": "notifications",
            "notifications": { "subscription": "drops-sub" },
        })))
        .contains("needs notifications.project or project")
    );
}

/// A loopback stand-in for workload identity federation exchanges: the
/// subject token URL, the STS token exchange and the service account
/// impersonation. Each request, lowercased, comes back on the channel.
fn federation_endpoints() -> (String, std::sync::mpsc::Receiver<String>) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("address"));
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let mut reader = BufReader::new(&stream);
            let (mut request, mut length) = (String::new(), 0);
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                let line = line.to_ascii_lowercase();
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap_or(0);
                }
                request.push_str(&line);
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            request.push_str(&String::from_utf8_lossy(&body).to_ascii_lowercase());
            let reply = if request.starts_with("get /subject") {
                r#"{"value":"oidc-subject-token"}"#
            } else if request.contains(":generateaccesstoken") {
                r#"{"accessToken":"impersonated-token","expireTime":"2999-01-01T00:00:00Z"}"#
            } else {
                r#"{"access_token":"federated-token","token_type":"Bearer","expires_in":3600}"#
            };
            let _ = write!(
                &stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            );
            let _ = tx.send(request);
        }
    });
    (base, rx)
}

/// An `external_account` configuration that federates a GitHub token
/// through `sts` and impersonates through `iam`.
fn external_account(sts: &str, iam: &str, subject: &str) -> String {
    json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/ci/providers/github",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": format!("{sts}/v1/token"),
        "service_account_impersonation_url": format!(
            "{iam}/v1/projects/-/serviceAccounts/watch@acme.iam.gserviceaccount.com:generateAccessToken"
        ),
        "credential_source": {
            "url": format!("{subject}/subject"),
            "headers": { "Authorization": "Bearer request-token" },
            "format": { "type": "json", "subject_token_field_name": "value" },
        },
    })
    .to_string()
}

/// The three requests of one federation exchange, in order.
fn assert_federated(requests: &std::sync::mpsc::Receiver<String>) {
    let next = || {
        requests
            .recv_timeout(Duration::from_secs(5))
            .expect("a request")
    };
    let subject = next();
    assert!(
        subject.starts_with("get /subject")
            && subject.contains("authorization: bearer request-token"),
        "{subject}"
    );
    let exchange = next();
    assert!(
        exchange.starts_with("post /v1/token")
            && exchange.contains("subject_token=oidc-subject-token"),
        "{exchange}"
    );
    let impersonation = next();
    assert!(
        impersonation.contains("authorization: bearer federated-token"),
        "{impersonation}"
    );
}

fn subscription(credentials_json: Option<String>) -> Subscription {
    Subscription {
        name: NAME.into(),
        project: Some("acme".into()),
        credentials_json,
        emulator_host: None,
        deadline_secs: 90,
        max_outstanding: 256,
    }
}

#[tokio::test]
async fn an_external_account_with_other_endpoints_is_refused_before_any_request() {
    let (base, requests) = federation_endpoints();
    let credentials = external_account(&base, &base, &base);
    let spec: WatchSpec = serde_json::from_value(json!({
        "bucket": "drops",
        "credentials_json": credentials,
    }))
    .unwrap();
    let storage = storage_auth(storage_config(&spec), spec.credentials_json.as_deref()).await;
    assert!(
        storage.as_ref().is_err_and(|e| e.contains("token_url")),
        "{:?}",
        storage.err()
    );
    let pubsub = subscription(Some(credentials)).config().await;
    assert!(
        pubsub.as_ref().is_err_and(|e| e.contains("token_url")),
        "{:?}",
        pubsub.err()
    );
    assert!(
        requests.recv_timeout(Duration::from_millis(500)).is_err(),
        "nothing is read or sent"
    );
}

#[tokio::test]
async fn an_external_account_configuration_authenticates_both_clients() {
    let (base, requests) = federation_endpoints();
    let credentials = external_account(&base, &base, &base);
    let spec: WatchSpec = serde_json::from_value(json!({ "bucket": "drops" })).unwrap();
    let file = StorageCredentials::new_from_str(&credentials)
        .await
        .expect("an external_account file parses");
    let storage = storage_with(storage_config(&spec), Some(file)).await;
    assert!(storage.is_ok(), "{:?}", storage.as_ref().err());
    assert_federated(&requests);

    let file = PubsubCredentials::new_from_str(&credentials)
        .await
        .expect("an external_account file parses");
    let config = gcloud_pubsub::client::ClientConfig {
        project_id: Some("acme".into()),
        ..gcloud_pubsub::client::ClientConfig::default()
    };
    let pubsub = pubsub_with(config, Some(file)).await;
    assert!(pubsub.is_ok(), "{:?}", pubsub.as_ref().err());
    assert_federated(&requests);
}
