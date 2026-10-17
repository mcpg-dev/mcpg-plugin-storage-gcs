# Cloud Storage Objects Watch Strategy — `dev.mcpg.storage.gcs`

> class `watch_strategy` · `native` · package `mcpg-plugin-storage-gcs` · artifact `libmcpg_plugin_storage_gcs.so` · Apache-2.0

Watches a Google Cloud Storage bucket. The `gcs_objects` watch strategy
reports each object created, modified or deleted under a prefix as the
shared `object.changed` event, the same shape a local folder, an SFTP
directory or an S3 bucket gives: `{op, store: "gcs", location:
"gs://bucket/prefix/", key, size, modifiedAt, etag, version}`. Keys are
relative to the prefix; `version` is the object generation.

```yaml
plugins:
  - id: dev.mcpg.storage.gcs
    class: watch_strategy
    kind: native
    source: { oci: "<registry>/plugins/storage-gcs:protocol-1" }

mcp:
  capabilities:
    events:
      - name: partner.drops
        description: A CSV arrived in, changed in, or left the partner drop prefix.
        source:
          watch:
            strategy:
              type: plugin
              kind: gcs_objects
              bucket: partner-drops
              prefix: incoming
              pattern: "**/*.csv"
              notifications:
                subscription: projects/acme-prod/subscriptions/partner-drops-events
            event_type: object.changed
```

It runs in one of two modes:

- **`list`** (default). The watch lists the prefix every `interval_ms` and
  reports what changed since the listing the gateway stored. An overwrite is
  a modification: it is a new generation. A restart resumes exactly,
  deletions included, while the listing fits `snapshot_bytes` (about 2 000
  objects); past that it records a gap and takes the listing it finds as its
  baseline. With `notifications`, each message on the subscription makes the
  watch list at once and is acknowledged; the listing stays what reports, so
  a lost notification costs only latency.
- **`notifications`**. The watch reads only the subscription, for a bucket
  too large to list. `OBJECT_FINALIZE` is `created`, or `modified` when it
  replaced a generation; `OBJECT_DELETE` and `OBJECT_ARCHIVE` are `deleted`,
  unless a newer write replaced the object, which reports itself; a metadata
  update is no object change. A notification is acknowledged once the
  gateway has stored its event, and nacked when it has not done so in
  `ack_timeout_ms`; the event id comes from the object generation, so a
  redelivered notification is stored once. Notifications outside the
  bucket, prefix or pattern are acknowledged without an event.

| Field | Type | Default | Notes |
|---|---|---|---|
| `bucket` | string (required) | — | |
| `prefix` | string | `""` | The folder watched; a `/` is added when missing. |
| `project` | string | from the credentials | Also the project of a bare subscription id. |
| `credentials_json` | string | Application Default Credentials | A Google credentials file, as JSON, resolved before the watch starts (`${secret.X}`, `vault://…`); `cred://` is rejected. Used for Cloud Storage and Pub/Sub. See [Credentials](#credentials). |
| `endpoint_url` | string | `https://storage.googleapis.com` | Another Cloud Storage endpoint. |
| `anonymous` | bool | `false` | Send no credentials to Cloud Storage: a public bucket or an emulator. |
| `mode` | `list` \| `notifications` | `list` | How the watch learns of changes. |
| `recursive` | bool | `true` | Objects in "subfolders" of the prefix too. |
| `pattern` | string | — | Keep only the names, relative to the prefix, that match this glob: `*` stays in a folder, `**` crosses them. |
| `notifications.subscription` | string | — | A Pub/Sub subscription to the bucket's notification topic (`JSON_API_V1` payloads); its id or full name. Required in `notifications` mode. |
| `notifications.project`, `notifications.emulator_host` | string | the watch's project / none | Where the subscription is. |
| `interval_ms` | int | `60000` | Listing cadence, at least 250. |
| `timeout_ms` | int | `30000` | Budget of one listing. |
| `max_files` | int | `10000` | Most objects listed (1 to 100000): the first in name order. |
| `start_at` | `now` \| `existing` | `now` | `existing` reports what is there at a first start as created. |
| `snapshot_bytes`, `max_in_flight`, `ack_timeout_ms` | int | `131072`, `256`, `60000` | As for every inventory and queue source; `ack_timeout_ms` at most 570000. |

The bucket's notification configuration (`gcloud storage buckets
notifications create`) is set up on Google Cloud, not here.

## Permissions

In `list` mode, the watch needs `storage.objects.list` on the bucket, for
example through `roles/storage.objectViewer`. In `notifications` mode, it
does not read the bucket.

With a subscription, in either mode, the watch needs
`roles/pubsub.subscriber` on the subscription. Its
`pubsub.subscriptions.consume` permission covers StreamingPull,
acknowledgements and nacks. Before it subscribes, the watch checks that the
subscription exists. This check needs `pubsub.subscriptions.get`.
`roles/pubsub.subscriber` does not have it; `roles/pubsub.viewer` has it.
When the check is refused, the watch logs this at debug level and
subscribes. It then watches the new stream for 2 seconds. If the
subscription is missing, StreamingPull ends the stream, and the watch
reports `the Pub/Sub stream ended before its first message: subscription …
may not exist, or the watcher may lack pubsub.subscriptions.consume on it;
the subscription check was refused: …`, with the status message of the
refused check. In `list` mode the watch then opens the stream again with a
backoff that grows to 30 seconds. In `notifications` mode it does so when
the stream ends in the 2 seconds; when the stream ends later, it opens the
stream again after 500 ms each time. With `pubsub.subscriptions.get` on the
project, for example `roles/pubsub.viewer` on the project, the watch
reports `Pub/Sub subscription … does not exist` before it subscribes.

## Credentials

Without `credentials_json`, the watch uses Application Default
Credentials; `anonymous` sends none to Cloud Storage. It reads, in this
order: the JSON in `GOOGLE_APPLICATION_CREDENTIALS_JSON` (plain or
base64), the file that `GOOGLE_APPLICATION_CREDENTIALS` names, gcloud's
`~/.config/gcloud/application_default_credentials.json`, and then the
metadata server.

| Credentials | Supported | Notes |
|---|---|---|
| `service_account` key | yes | In `credentials_json` or an ADC file. |
| `authorized_user` (`gcloud auth application-default login`) | yes | In `credentials_json` or an ADC file. |
| `external_account` (Workload Identity Federation) | yes | In `credentials_json` or an ADC file. The subject token comes from the `credential_source` file, URL or AWS environment. An `executable` credential source is not supported. |
| The metadata server | yes | When there is no credentials file: Compute Engine, GKE Workload Identity, Cloud Run. |
| `impersonated_service_account` (`gcloud auth application-default login --impersonate-service-account`) | no | Authentication fails with `unsupported account impersonated_service_account`. Use an `external_account` file with `service_account_impersonation_url`, or a key of the target service account. |

An `external_account` file in `credentials_json` makes the gateway read the
file or URL that its `credential_source` names, and send the result to its
`token_url`. So the watch does not start unless `token_url` and
`token_info_url` start with `https://sts.googleapis.com/` and
`service_account_impersonation_url` starts with
`https://iamcredentials.googleapis.com/`. An ADC file is not checked: the
gateway's operator sets it.

In `credentials_json`, only a `service_account` key carries a project. With
other credentials there, a bare `notifications.subscription` id needs
`notifications.project` or `project`, or the watch does not start.

## Build / test

```bash
cargo build  -p mcpg-plugin-storage-gcs
cargo test   -p mcpg-plugin-storage-gcs                              # unit tests
cargo test   -p mcpg-plugin-storage-gcs --features integration-tests # fake-gcs-server + Pub/Sub emulator (docker)
cargo clippy -p mcpg-plugin-storage-gcs --all-targets -- -D warnings
```

The integration suite runs `gcs_objects` against fake-gcs-server, which
publishes its object events to a Pub/Sub emulator beside it: a listing
through creates, a same-size overwrite, a deletion and an exact restart; the
subscription alone through a create, a delete and a redelivery; and a
listing a notification wakes. With the same feature, the crate's own tests
start a notification pull on the Pub/Sub emulator: a missing subscription
fails the open when the watch checks it and when it cannot, and a
notification that comes while the start watches the stream is received
first.
