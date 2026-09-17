// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"context"
	"crypto/rsa"
	"fmt"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	v2 "github.com/redpanda-data/connect/v4/internal/impl/snowflake/streamingv2"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/pool"
)

const (
	ssopFieldAccount     = "account"
	ssopFieldURL         = "url"
	ssopFieldAccountHost = "account_host"
	ssopFieldUser        = "user"
	ssopFieldRole        = "role"
	ssopFieldDB          = "database"
	ssopFieldSchema      = "schema"
	ssopFieldPipe        = "pipe"
	ssopFieldKey         = "private_key"
	ssopFieldKeyFile     = "private_key_file"
	ssopFieldKeyPass     = "private_key_pass"
	ssopFieldBatching    = "batching"
	ssopFieldChannelName = "channel_name"
	ssopFieldOffsetToken = "offset_token"
	// ssopFieldCommitTimeout collapses the v1 snowflake_streaming output's
	// commit_backoff (initial_interval, max_interval, max_elapsed_time,
	// multiplier) plus its separately-named deprecated commit_timeout
	// override into a single wait budget: this output's WaitUntilCommitted takes one
	// context.Context and one time.Duration timeout, not a backoff
	// schedule.
	ssopFieldCommitTimeout = "commit_timeout"
	// ssopFieldTolerateRowErrors has no equivalent in the v1
	// snowflake_streaming output. It maps onto the Kafka Connector's
	// errors.tolerance (`none`/`all`): rows Snowflake rejects
	// asynchronously after append are always logged and metered, and this
	// field controls whether they also fail the batch. Default false
	// matches the Kafka Connector's own default (errors.tolerance=none):
	// fail loud on any newly-rejected row.
	ssopFieldTolerateRowErrors = "tolerate_row_errors"
)

// defaultChannelName builds the channel_name default used when channel_name
// is not configured: a single, constant, unpartitioned channel scoped to the
// fully-qualified pipe name. This output has no pooled/unordered mode to
// fall back to, so a single default channel -- rather than a per-partition
// one keyed off Kafka-specific metadata -- is the only default that works
// regardless of what kind of input is feeding this output. An operator who
// wants partition-aware parallelism opts in explicitly via channel_name; see
// its own doc comment for the pattern.
func defaultChannelName(db, schema, pipe string) string {
	return fmt.Sprintf("%s.%s.%s", db, schema, pipe)
}

func snowpipeStreamingPipeOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Beta().
		Version("4.110.0").
		Summary("Ingest data into Snowflake using Snowpipe Streaming, writing through a Snowflake pipe.").
		Description(`
Ingest data into Snowflake using Snowpipe Streaming, writing through a Snowflake pipe rather than directly into a table.

The `+"`snowflake_streaming`"+` output opens its channels on a table, which puts Snowflake's Streaming Transformations -- enrichment via a stream-static join, and server-side `+"`WHERE`"+`-clause filtering -- out of reach, since those only exist on a pipe's `+"`COPY INTO`"+` and a table has no such thing. This output opens its channels on a pipe instead, so any transformation defined on that pipe runs before rows land; the connector itself still sends rows untouched. If you don't need those transformations, `+"`snowflake_streaming`"+` remains the simpler choice -- it creates its destination table for you and needs no pipe set up first; this output never does either (see below).

This is an Enterprise feature: running it requires a valid Redpanda Enterprise Edition license that includes the Connect product. See https://docs.redpanda.com/redpanda-connect/get-started/licensing/[the licensing docs^] if you haven't already got one -- a config that lints clean will still fail at startup without it.

This output never issues `+"`CREATE PIPE`"+` or `+"`CREATE TABLE`"+` (the `+"`snowflake_streaming`"+` output can create its target table): the `+"`"+ssopFieldPipe+"`"+` field names an object that must already exist. Create the table and pipe yourself first -- the pipe's `+"`SELECT`"+` is where any casting, projection, renaming or filtering happens -- before pointing this output at it; a missing pipe fails at connector startup rather than at the first message.

Moving an existing `+"`snowflake_streaming`"+` pipeline across involves more than adding `+"`"+ssopFieldPipe+"`"+`: several fields are removed outright, and `+"`"+ssopFieldCommitTimeout+"`"+` keeps its name but changes what it means. See the xref:guides:migrating-to-snowflake-streaming-pipe.adoc[migration guide] before carrying a config forward unchanged.

[%header,format=dsv]
|===
Snowflake column type:Allowed format in Redpanda Connect
CHAR, VARCHAR:string
BINARY:[]byte
NUMBER:any numeric type, string
FLOAT:any numeric type
BOOLEAN:bool,any numeric type,string parsable according to `+"`strconv.ParseBool`"+`
TIME,DATE,TIMESTAMP:unix or RFC 3339 with nanoseconds timestamps
VARIANT,ARRAY,OBJECT:any data type is converted into JSON
GEOGRAPHY,GEOMETRY: Not supported
|===

Authentication can be configured using a https://docs.snowflake.com/en/user-guide/key-pair-auth[RSA Key Pair^].

There are https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#limitations[limitations^] of what data types can be loaded into Snowflake using this method.
`+service.OutputPerformanceDocs(true, true)+`

Throughput on this output is governed by batch size, and batch size is capped by the *input's* `+"`max_yield_batch_bytes`"+` field, not by this output's `+"`batching`"+` policy -- nothing set here raises it. Its default, `+"`32KB`"+`, is far below what this connector can carry, so raising it is the primary throughput lever. Setting a `+"`batching`"+` policy on this output does not merely fail to help: with a multi-partition input it pools every partition's yields into one request, serializing throughput across partitions and risking the 4MiB limit below -- so leaving it unset is the recommendation here, not merely the default.

Whatever value is chosen there must stay under this output's 4MiB uncompressed per-request limit, and must not exceed the input's own `+"`partition_buffer_bytes`"+`. A batch over the 4MiB limit is rejected outright rather than split, and because a batch rejected for size is rejected identically on every retry, exceeding the limit wedges that partition indefinitely rather than merely running slowly.
`).
		Fields(
			service.NewStringField(ssopFieldAccount).
				Description(`The Snowflake https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier[Account name^]. Which should be formatted as `+"`<orgname>-<account_name>`"+` where `+"`<orgname>`"+` is the name of your Snowflake organization and `+"`<account_name>`"+` is the unique name of your account within your organization.
`).
				ShortDescription("The Snowflake account name, formatted as orgname-account_name.").Example("ORG-ACCOUNT"),
			service.NewStringField(ssopFieldURL).
				Description(`Override the URL used to connect to Snowflake for BOTH the control-plane and ingest-plane connections, which is `+"`https://ORG-ACCOUNT.snowflakecomputing.com`"+` by default. Setting this skips Snowflake's normal ingest-hostname discovery entirely, so it is intended for local and test deployments where discovery would resolve to something unreachable, not for production accounts.

This is NOT the field for `+"`PrivateLink`"+` accounts or other non-standard hostnames: use `+"`"+ssopFieldAccountHost+"`"+` for that, which keeps ingest-hostname discovery working. If both `+"`"+ssopFieldURL+"`"+` and `+"`"+ssopFieldAccountHost+"`"+` are set, `+"`"+ssopFieldURL+"`"+` wins.
`).Optional().Example("http://localhost:8080").Advanced(),
			service.NewStringField(ssopFieldAccountHost).
				Description(`Override the control-plane host used to reach Snowflake, without changing how the ingest (data-plane) host is discovered: Snowflake's `+"`/v2/streaming/hostname`"+` discovery still runs normally against this host. Use this for accounts whose hostname doesn't follow the default `+"`<account>.snowflakecomputing.com`"+` pattern derived from `+"`"+ssopFieldAccount+"`"+` -- most commonly https://docs.snowflake.com/en/user-guide/admin-privatelink[PrivateLink^] accounts (`+"`<account>.privatelink.snowflakecomputing.com`"+`), and non-standard deployments such as a QA account.

If `+"`"+ssopFieldURL+"`"+` is also set, `+"`"+ssopFieldURL+"`"+` takes precedence and this field has no effect: `+"`"+ssopFieldURL+"`"+` overrides both planes and skips discovery, which its own description explains is for local/test deployments, not PrivateLink. Leaving this unset preserves the previous behaviour, deriving the control-plane host as `+"`<account>.snowflakecomputing.com`"+`.
`).Optional().Example("my-account.privatelink.snowflakecomputing.com").Advanced(),
			service.NewStringField(ssopFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user.").
				ShortDescription("The user to run the Snowpipe Stream as."),
			service.NewStringField(ssopFieldRole).Description("The role to ingest as. The connector requests a session-scoped token for this role, so Snowflake authorizes every streaming call against it rather than against the `user`'s default role -- setting a least-privilege role here does restrict ingestion. It needs the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] on the target pipe and table; a role that cannot reach the pipe fails at startup, naming the role. Note that the effective privileges are this role's whole inherited hierarchy.").
				ShortDescription("The role to ingest as; sent as a session-scoped token, so it does constrain ingestion privileges.").Example("SNOWPIPE_STREAMING_ROLE"),
			service.NewStringField(ssopFieldDB).Description("The Snowflake database to ingest data into.").Example("MY_DATABASE"),
			service.NewStringField(ssopFieldSchema).Description("The Snowflake schema to ingest data into.").Example("PUBLIC"),
			service.NewStringField(ssopFieldPipe).Description("The Snowflake pipe to ingest data into. The pipe must already exist -- this output never issues `CREATE PIPE`, so a missing pipe fails at startup rather than at the first message.").Example("MY_PIPE"),
			service.NewStringField(ssopFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").
				ShortDescription("PEM encoded private RSA key for authenticating with Snowflake. Either this or private_key_file is required.").Optional().Secret(),
			service.NewStringField(ssopFieldKeyFile).Description("The file to load the private RSA key from. This should be a `.p8` PEM encoded file. Either this or `private_key` must be specified.").
				ShortDescription("File to load the private RSA key from, as a .p8 PEM file. Either this or private_key is required.").Optional(),
			service.NewStringField(ssopFieldKeyPass).Description("The RSA key passphrase if the RSA key is encrypted.").Optional().Secret(),
			service.NewBatchPolicyField(ssopFieldBatching),
			service.NewOutputMaxInFlightField().Default(4),
			service.NewInterpolatedStringField(ssopFieldChannelName).
				Description(`The channel to write through. Duplicate channel names will result in errors and prevent multiple instances of Redpanda Connect from writing at the same time. This interpolation is executed independently for every message in a batch, not just the first: messages are grouped by their resolved channel name (preserving each message's relative order within its group) and each group is appended, deduplicated (when `+"`"+ssopFieldOffsetToken+"`"+` is set) and committed on its own. It's still recommended to batch at the input level so that a batch contains messages for as few channels as possible, since a batch spanning several channels is handled correctly but round-trips to Snowflake once per channel instead of once for the whole batch.

If not set, defaults to a single, constant, unpartitioned channel scoped to this output's pipe: `+"`<database>.<schema>.<pipe>`"+`.

*The exactly-once requirement this field exists to satisfy:* a channel must only ever be written to by one ordered source, with at most one batch in flight to it at a time -- `+"`"+ssopFieldOffsetToken+"`"+`'s dedup check assumes tokens arrive in increasing order, and has no way to tell "this was already committed by me" apart from "a different, unrelated writer just leapfrogged me". Two unrelated sources sharing one channel breaks that assumption regardless of how carefully each one is itself ordered.

For a partitioned Kafka/Redpanda input, scope this per partition so each partition gets its own channel and its own independent offset-token sequence:

`+"```"+`
channel_name: "<pipe>-p${! @kafka_partition }"
`+"```"+`

A genuinely per-partition-ordered reader (Kafka/Redpanda consumer groups included) never dispatches a partition's next batch until the current one has been acknowledged, so this pattern is safe at any `+"`max_in_flight`"+` -- the concurrency only ever plays out *across* partitions, never within one. The prefix must be stable for a given partition across restarts and across hosts: a per-host value such as `+"`channel-${HOST}`"+` breaks exactly-once, since a consumer-group rebalance would then hand the partition to a channel with no committed history, and every row the previous host already landed gets re-sent as new.

If you're not sure an input provides that same one-batch-in-flight-per-source guarantee, or the input has no natural partition key at all, the always-safe fallback is this field's own default (one shared channel) with `+"`max_in_flight: 1`"+` set explicitly -- slower, but there is then only ever one writer, period.

If that requirement is violated anyway, this output does not silently drop the affected rows: it fails the batch with an error naming the channel and the offset tokens involved, and increments the `+"`snowflake_out_of_order_submissions`"+` metric. That error will keep recurring on retry for as long as the same concurrent writers keep contending for the channel -- it is a configuration problem to fix (per the guidance above), not a transient failure to wait out.

NOTE: There is a limit of 10,000 streams per table - if using more than 10k streams please reach out to Snowflake support.`).
				Optional().
				Advanced().
				Examples(`MYDB.PUBLIC.BETS_PIPE-p${!@kafka_partition}`),
			service.NewInterpolatedStringField(ssopFieldOffsetToken).
				Description(`The offset token to use for exactly once delivery of data in the pipeline. When data is sent on a channel, each message in a batch's offset token
is compared numerically to the latest token for a channel when both parse as integers (as Kafka offsets do), and lexicographically otherwise. If the offset token is
not newer than the latest in the channel, it's assumed the message is a duplicate and is dropped. This means it is *very important* to have ordered delivery to the
output, any out of order messages to the output will be seen as duplicates and dropped. Specifically this means that retried messages could be seen as duplicates if
later messages have succeeded in the meantime, so in most circumstances a dead letter queue output should be employed for failed messages.

NOTE: It's assumed that messages within a batch are in increasing order by offset token.

If not set, this output does not deduplicate at all: every row is appended and committed as received. That's genuine
at-least-once delivery, not an error condition -- omit this field entirely for any input that has no natural per-partition
offset to give it. For a Kafka/Redpanda-shaped input that wants exactly-once, `+"`${! @kafka_offset }`"+` (the same value the Kafka
Connector uses) is a common choice, shown as an example below rather than assumed by default; any other input can supply its
own monotonic per-partition value instead, such as a Postgres LSN. Note that leaving this unset scales the usual
at-least-once duplicate risk to every channel a single batch happens to touch, not just one -- a batch spanning several
channels (see `+"`"+ssopFieldChannelName+"`"+`) that partially succeeds before a retry can duplicate rows on each channel that already
landed, since there is no offset token to filter any of them back out on redelivery.

For more information about offset tokens, see https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#offset-tokens[^Snowflake Documentation]`).
				ShortDescription("The offset token used for exactly-once delivery, compared against the latest token for a channel. If unset, this output doesn't deduplicate at all (at-least-once).").
				Optional().
				Advanced().
				Examples(`${! @kafka_offset }`, `postgres-${!@lsn}`),
			service.NewDurationField(ssopFieldCommitTimeout).
				Description("The maximum total time to wait for data to be committed to Snowflake before returning an error. If zero then no limit is used.").
				ShortDescription("The maximum time to wait for data to be committed before returning an error.").
				Default("60s").
				Advanced(),
			service.NewBoolField(ssopFieldTolerateRowErrors).
				Description(`Whether to tolerate rows Snowflake rejects after a batch has been appended (a malformed or uncastable field, visible only in the channel's asynchronous status as `+"`rows_error_count > 0`"+` alongside a `+"`channel_status_code`"+` that still reads `+"`SUCCESS`"+`). This is this output's equivalent of the Kafka Connector's `+"`errors.tolerance`"+` setting: `+"`false`"+` (the default) matches the Kafka Connector's own default of `+"`errors.tolerance=none`"+` -- any newly-rejected row fails the batch with an error naming the channel, the new/total/initial error counts, and Snowflake's last error message. `+"`true`"+` matches `+"`errors.tolerance=all`"+`: the same rejection instead only logs a warning and continues. In both settings the `+"`snowflake_rows_error_count`"+` metric is incremented by the newly-rejected count. Rejected rows never reach a dead-letter queue either way -- neither this output nor the Kafka Connector routes server-side (post-append) rejections to one; the Kafka Connector's DLQ is wired only to client-side pre-append validation failures. Note that failing the batch (the `+"`false`"+` default) does not cause the rejected rows themselves to be retried: Snowflake has already permanently rejected them, and a redelivered batch dedupes against the channel's already-advanced offset token to nothing. The failure exists to alert an operator, via the error and the metric, not to drive a retry that couldn't help.`).
				ShortDescription("Whether to tolerate rows Snowflake rejects (equivalent to the Kafka Connector's `errors.tolerance`); false (default) fails the batch, true only warns.").
				Default(false).
				Advanced(),
		).
		// A second, separate LintRule call here would silently discard this
		// one instead of adding to it -- ConfigSpec.LintRule assigns the
		// mapping string outright (FieldSpec.LinterBlobl), it doesn't
		// compose across calls. All rules for this component must live in
		// this single match block; a mistake here previously left the
		// private_key/private_key_file exclusion dead (overwritten by a
		// second .LintRule() call below it), passing silently on a config
		// that set both.
		LintRule(`root = match {
  this.exists("private_key") && this.exists("private_key_file") => [ "both `+"`private_key`"+` and `+"`private_key_file`"+` can't be set simultaneously" ],
  !this.exists("private_key") && !this.exists("private_key_file") => [ "exactly one of `+"`private_key`"+` or `+"`private_key_file`"+` must be set" ],
}`).
		Example(
			"Simple at-least-once ingestion",
			`The minimal config: no `+"`offset_token`"+` or `+"`channel_name`"+`. Neither is required, and
neither implies anything about the input -- this works the same way whether the input is Kafka-shaped, a
plain file, or (as here) a synthetic generator. Rows land at-least-once, with no dedup on retry, through a
single channel named after the pipe. That's a fine starting point for getting something running; add
`+"`offset_token`"+` later if you need exactly-once, or `+"`channel_name`"+` if you need more than one
channel writing in parallel.`,
			`
input:
  generate:
    mapping: 'root = { "id": uuid_v4(), "amount": random_int(min: 1, max: 1000) }'
    interval: 1s
output:
  snowflake_streaming_pipe:
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: SNOWPIPE_STREAMING_ROLE
    database: "MYDATABASE"
    schema: "PUBLIC"
    pipe: "MY_PIPE"
    private_key_file: "my/private/key.p8"
`).
		Example(
			"Exactly once CDC into Snowflake",
			`How to send data from a PostgreSQL table into Snowflake exactly once using Postgres Logical Replication.

NOTE: This uses one unpartitioned channel (no `+"`"+ssopFieldChannelName+"`"+` set), which is only safe for exactly-once because `+"`"+"max_in_flight: 1"+"`"+` guarantees at most one writer to it -- see `+"`"+ssopFieldChannelName+"`"+`'s own documentation for why that pairing matters. Removing `+"`"+ssopFieldOffsetToken+"`"+` is a safer option than getting that pairing wrong: it switches Redpanda Connect to at-least-once delivery instead.`,
			`
input:
  postgres_cdc:
    dsn: postgres://foouser:foopass@localhost:5432/foodb
    schema: "public"
    slot_name: "my_repl_slot"
    tables: ["my_pg_table"]
    # We want very large batches - each batch will be sent to Snowflake individually
    # so to optimize query performance we want as big of files as we have memory for.
    # count has no byte-size awareness: 50000 rows can exceed this output's 4MiB
    # uncompressed per-request limit depending on row size, and a batch rejected for
    # size fails identically on every retry. Size count against your actual row size.
    # This example has no fallback (see the Redpanda example below for one) -- with a
    # single channel and no fallback, an oversized batch wedges the whole pipeline, not
    # just one partition among many.
    batching:
      count: 50000
      period: 45s
    # Prevent multiple batches from being in flight at once, so that we never send
    # a batch while another batch is being retried, this is important to ensure that
    # the Snowflake Snowpipe Streaming channel does not see older data - as it will
    # assume that the older data is already committed.
    checkpoint_limit: 1
output:
  snowflake_streaming_pipe:
    # We use the log sequence number in the WAL from Postgres to ensure we
    # only upload data exactly once, these are already lexicographically
    # ordered.
    offset_token: "${!@lsn}"
    # This whole pipeline is one ordered stream feeding one channel (no
    # channel_name set below, so every message shares the single default
    # channel). Setting max_in_flight above 1 would let more than one
    # WriteBatch call race for that same channel concurrently, which can
    # silently drop rows out of order -- see channel_name's own docs for
    # why. 1 is the always-safe choice whenever a single channel has no
    # per-partition split to lean on.
    max_in_flight: 1
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: SNOWPIPE_STREAMING_ROLE
    database: "MYDATABASE"
    schema: "PUBLIC"
    pipe: "MY_PG_PIPE"
    private_key_file: "my/private/key.p8"
`).
		Example(
			"Ingesting data exactly once from Redpanda",
			`How to ingest data from Redpanda with consumer groups, decode the schema using the schema registry, then write the corresponding data into Snowflake exactly once, through a pipe (rather than directly into a table) so that any transformation or filtering defined on the pipe's COPY INTO runs before the rows land.

NOTE: This is the pattern to copy for any partitioned Kafka/Redpanda input: `+"`"+ssopFieldChannelName+"`"+` is scoped per partition below, which is what makes exactly-once hold here -- see that field's own documentation for exactly what it protects and why. Removing `+"`"+ssopFieldOffsetToken+"`"+` is a safer option than getting that wrong: it switches Redpanda Connect to at-least-once delivery instead.`,
			`
input:
  redpanda:
    topics: ["my_topic_going_to_snow"]
    consumer_group: "redpanda_connect_to_snowflake"
    # We want very large batches - each batch will be sent to Snowflake individually
    # so to optimize query performance we want as big of files as we have memory for
    fetch_max_bytes: 100MiB
    fetch_min_bytes: 50MiB
    partition_buffer_bytes: 100MiB
pipeline:
  processors:
    - schema_registry_decode:
        url: "redpanda.example.com:8081"
        basic_auth:
          enabled: true
          username: MY_USER_NAME
          password: "${REDPANDA_SCHEMA_REGISTRY_PASSWORD}"
output:
  fallback:
    - snowflake_streaming_pipe:
        # Each Kafka/Redpanda partition gets its own channel, so a batch
        # from one partition never shares a channel with a batch from
        # another -- see channel_name's own documentation for why that's
        # the requirement this is satisfying, and why a real per-partition-
        # ordered consumer group makes it safe to run this at any
        # max_in_flight (unlike the Postgres example above, which is a
        # single unpartitioned channel and needs max_in_flight: 1 instead).
        channel_name: "MYDB.PUBLIC.BETS_PIPE-p${!@kafka_partition}"
        # Deliberately no batching policy on this output. With a multi-partition input, an
        # output batching policy pools every partition's yields into one HTTP request, and
        # its byte_size trigger is a threshold rather than a hard cap: the shipped batch can
        # exceed it by roughly one more partition's yield, enough to blow past this output's
        # 4MiB limit and wedge a partition permanently. Leaving batching unset here removes
        # that failure class -- batch size is governed solely by the input's own
        # max_yield_batch_bytes (default 32KB). Raise that instead of adding batching here.
        # Tokens that parse as int64 are compared numerically, so no
        # zero-padding is needed for correct ordering. offset_token has no
        # default -- omitting it switches this output to at-least-once
        # delivery instead of exactly-once, so set it explicitly here.
        offset_token: ${! @kafka_offset }
        account: "MYSNOW-ACCOUNT"
        user: MYUSER
        role: SNOWPIPE_STREAMING_ROLE
        database: "MYDATABASE"
        schema: "PUBLIC"
        pipe: "MYPIPE"
        private_key_file: "my/private/key.p8"
    # In order to prevent delivery orders from messing with the order of delivered records
    # it's important that failures are immediately sent to a dead letter queue and not retried
    # to Snowflake. See the ordering documentation for the "redpanda" input for more details.
    - retry:
        output:
          redpanda:
            topic: "dead_letter_queue"
`,
		).
		Example(
			"HTTP Server to push data to Snowflake",
			`This example demonstrates how to create an HTTP server input that can receive HTTP PUT requests
with JSON payloads, that are buffered locally then written to Snowflake in batches.

NOTE: This example uses a buffer to respond to the HTTP request immediately, so it's possible that failures to deliver data could result in data loss.
See the documentation about xref:components:buffers/memory.adoc[buffers] for more information, or remove the buffer entirely to respond to the HTTP request only once the data is written to Snowflake.`,
			`
input:
  http_server:
    path: /snowflake
buffer:
  memory:
    # Max inflight data before applying backpressure
    limit: 524288000 # 50MiB
    # Batching policy: keep each flush well under this connector's 4MiB
    # per-request limit (streamingv2.Client's request size limit). That cap
    # is on the uncompressed row body, is checked by rejecting the batch
    # outright rather than splitting it, and a request that is rejected for
    # being oversized will be rejected identically on every retry -- with
    # max_in_flight left at its default of 1, that wedges the pipeline
    # indefinitely rather than merely failing once. 2MiB gives real headroom.
    batch_policy:
      enabled: true
      byte_size: 2097152 # 2MiB
      period: "10s"
output:
  snowflake_streaming_pipe:
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: SNOWPIPE_STREAMING_ROLE
    database: "MYDATABASE"
    schema: "PUBLIC"
    pipe: "MYPIPE"
    private_key_file: "my/private/key.p8"
    # By default there is only a single channel per output pipe allowed
    # if we want to have multiple Redpanda Connect streams writing data
    # then we need a unique channel name per stream. We'll use the host
    # name to get a unique name in this example.
    channel_name: "snowflake-channel-for-${HOST}"
`,
		)
}

func init() {
	service.MustRegisterBatchOutput(
		"snowflake_streaming_pipe",
		snowpipeStreamingPipeOutputConfig(),
		newSnowpipeStreamingPipeOutput)
}

func newSnowpipeStreamingPipeOutput(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (
	output service.BatchOutput,
	batchPolicy service.BatchPolicy,
	maxInFlight int,
	err error,
) {
	if err = license.CheckRunningEnterprise(mgr); err != nil {
		return
	}
	if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
		return
	}
	if batchPolicy, err = conf.FieldBatchPolicy(ssopFieldBatching); err != nil {
		return
	}
	output, err = newSnowflakeStreamerPipe(conf, mgr)
	return
}

func newSnowflakeStreamerPipe(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (service.BatchOutput, error) {
	keypass := ""
	if conf.Contains(ssopFieldKeyPass) {
		pass, err := conf.FieldString(ssopFieldKeyPass)
		if err != nil {
			return nil, err
		}
		keypass = pass
	}
	var rsaKey *rsa.PrivateKey
	if conf.Contains(ssopFieldKey) {
		key, err := conf.FieldString(ssopFieldKey)
		if err != nil {
			return nil, err
		}
		rsaKey, err = getPrivateKey([]byte(key), keypass)
		if err != nil {
			return nil, err
		}
	} else if conf.Contains(ssopFieldKeyFile) {
		keyFile, err := conf.FieldString(ssopFieldKeyFile)
		if err != nil {
			return nil, err
		}
		rsaKey, err = getPrivateKeyFromFile(mgr.FS(), keyFile, keypass)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("one of `%s` or `%s` is required", ssopFieldKey, ssopFieldKeyFile)
	}
	account, err := conf.FieldString(ssopFieldAccount)
	if err != nil {
		return nil, err
	}
	// v2.Config splits the old single `url` override into BaseURL (used
	// verbatim for both planes, skipping ingest-host discovery) and
	// AccountHost (control-plane host only; ingest-host discovery still
	// runs normally). Both are populated independently below: accountBase()
	// prefers BaseURL when set, so if the user sets both `url` and
	// `account_host`, `url` wins without any special-casing here.
	var accountHost, baseURL string
	if conf.Contains(ssopFieldAccountHost) {
		accountHost, err = conf.FieldString(ssopFieldAccountHost)
		if err != nil {
			return nil, err
		}
	} else {
		accountHost = account + ".snowflakecomputing.com"
	}
	if conf.Contains(ssopFieldURL) {
		baseURL, err = conf.FieldString(ssopFieldURL)
		if err != nil {
			return nil, err
		}
		if _, err := neturl.Parse(baseURL); err != nil {
			return nil, fmt.Errorf("invalid url: %w", err)
		}
	}
	user, err := conf.FieldString(ssopFieldUser)
	if err != nil {
		return nil, err
	}
	role, err := conf.FieldString(ssopFieldRole)
	if err != nil {
		return nil, err
	}
	db, err := conf.FieldString(ssopFieldDB)
	if err != nil {
		return nil, err
	}
	schema, err := conf.FieldString(ssopFieldSchema)
	if err != nil {
		return nil, err
	}
	pipe, err := conf.FieldString(ssopFieldPipe)
	if err != nil {
		return nil, err
	}

	hasChannelName := conf.Contains(ssopFieldChannelName)
	var channelName *service.InterpolatedString
	if hasChannelName {
		channelName, err = conf.FieldInterpolatedString(ssopFieldChannelName)
		if err != nil {
			return nil, err
		}
	}

	var offsetToken *service.InterpolatedString
	if conf.Contains(ssopFieldOffsetToken) {
		offsetToken, err = conf.FieldInterpolatedString(ssopFieldOffsetToken)
		if err != nil {
			return nil, err
		}
	}
	// offsetToken stays nil when offset_token isn't configured, exactly like
	// the v1 snowflake_streaming output's own offsetToken field. WriteBatch
	// treats a nil offsetToken as "skip dedup/ordering, deliver
	// at-least-once" rather than an error -- see the offset_token field's
	// Description above. Never substitute a synthetic/counter-derived token
	// here: a counter resets on restart and would make the dedup guard
	// silently drop rows already committed under a higher token.

	commitTimeout, err := conf.FieldDuration(ssopFieldCommitTimeout)
	if err != nil {
		return nil, err
	}

	tolerateRowErrors, err := conf.FieldBool(ssopFieldTolerateRowErrors)
	if err != nil {
		return nil, err
	}

	// Normalize role, db and schema as they are case-sensitive in the API calls.
	// Maybe we should use the golang SQL driver for SQL statements so we don't have
	// to handle this, instead of the REST API directly.
	role = strings.ToUpper(role)
	db = strings.ToUpper(db)
	schema = strings.ToUpper(schema)

	if channelName == nil {
		// No channel_name configured: fall back to a single, constant,
		// unpartitioned channel for the whole output. This output has no
		// pooled/unordered mode to fall back to, so this is the only default
		// that works regardless of what's feeding this output.
		// Partition-aware parallelism is opt-in via channel_name; see its
		// own doc comment for the pattern.
		expr := defaultChannelName(db, schema, pipe)
		channelName, err = service.NewInterpolatedString(expr)
		if err != nil {
			return nil, fmt.Errorf("internal error building default %s template: %w", ssopFieldChannelName, err)
		}
	}

	client, err := v2.NewClient(v2.Config{
		Account:     account,
		AccountHost: accountHost,
		BaseURL:     baseURL,
		User:        user,
		Role:        role,
		PrivateKey:  rsaKey,
		Database:    db,
		Schema:      schema,
		Pipe:        pipe,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create streaming client: %w", err)
	}
	if err := client.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to connect streaming client: %w", err)
	}

	indexed := &snowpipeIndexedOutputPipe{
		db:                db,
		schema:            schema,
		channelName:       channelName,
		offsetToken:       offsetToken,
		commitTimeout:     commitTimeout,
		tolerateRowErrors: tolerateRowErrors,
		logger:            mgr.Logger(),
		metrics:           newSnowpipePipeMetrics(mgr.Metrics()),
		openChannelFn: func(ctx context.Context, name string) (v2.IngestChannel, error) {
			return client.OpenChannel(ctx, name)
		},
	}
	indexed.channelPool = pool.NewIndexed(func(ctx context.Context, name string) (v2.IngestChannel, error) {
		return indexed.openChannelFn(ctx, name)
	})
	return indexed, nil
}

type snowpipeIndexedOutputPipe struct {
	channelPool pool.Indexed[v2.IngestChannel]

	openChannelFn func(ctx context.Context, name string) (v2.IngestChannel, error)

	db, schema string
	// offsetToken is nil when offset_token isn't configured. writeChannelGroup
	// treats that as "skip dedup/ordering, deliver at-least-once" rather than
	// an error -- see the offset_token field's Description.
	offsetToken, channelName *service.InterpolatedString
	commitTimeout            time.Duration
	tolerateRowErrors        bool
	logger                   *service.Logger
	metrics                  *snowpipePipeMetrics

	// noDedupCommitToken hands WaitUntilCommitted something to poll for when
	// offsetToken is nil, so it can still tell "this specific append landed"
	// from "some earlier append on this channel already landed". Reusing a
	// fixed value (or the empty string) on every call can't make that
	// distinction: once any commit carrying that value lands, every later
	// call would see it immediately and return before its own append ever
	// committed. This counter is process-local and shared across every
	// channel this output writes to (global uniqueness is simplest to
	// reason about, and no less correct than scoping it per channel). It
	// makes no durability claim beyond "this call's append landed" -- unlike
	// offsetToken, it is never compared against anything from a prior
	// process run, because with offsetToken nil there is no dedup pass to
	// do that comparison for.
	noDedupCommitToken atomic.Int64

	// submittedWatermarks tracks, per channel name, the highest
	// endOffsetToken any writeChannelGroup call in this process has itself
	// already attempted to append -- see checkSubmissionOrder's doc comment
	// for the class of silent data loss this catches that the channel's own
	// committed token (queried fresh from Snowflake on every call) can't:
	// that value reflects whoever committed most recently, with no way to
	// tell "this is my own batch landing a second time" apart from "a
	// different, logically later batch already landed before mine got its
	// turn". This map answers that question locally instead, since only
	// this process's own submission history can.
	submittedWatermarks   map[string]string
	submittedWatermarksMu sync.Mutex
}

// checkSubmissionOrder guards the invariant every offset_token-bearing
// channel depends on -- see channel_name's own Description -- that at most
// one source, in increasing-token order, is ever writing to a given channel
// at a time. It is not a fairness mechanism and cannot make a genuinely
// disordered writer (misconfigured max_in_flight sharing one channel across
// partitions, or any other source that violates that invariant) safe: two
// truly independent, unordered sources have no shared token order for this
// check to enforce in the first place. What it does do is turn the
// resulting failure from silent -- rows dropped by the dedup filter below
// as if they were an ordinary already-committed retry, with no error, no
// warning, no metric -- into an immediate, attributable error the first
// time it happens, rather than passing a real bug or misconfiguration off
// as successful at-least-once delivery.
//
// That error is not transient: the watermark only ever advances, so a batch
// that lost the race can never win a later one by retrying with the same
// content -- it will fail this check again every time, for as long as the
// same concurrent writers keep advancing the watermark past it. An input
// that retries nacked batches forever with no dead-letter path will retry
// this one forever too, uselessly. See the error text below, which says so
// explicitly rather than reading like an ordinary retryable failure.
//
// end must be a token this call is *about* to submit (called once per
// writeChannelGroup invocation, before AppendRows), not one already
// committed elsewhere -- Acquire/Release's mutual exclusion on the channel
// object means no two calls for the same name run this concurrently, so
// there is no separate locking concern beyond protecting the map itself.
func (o *snowpipeIndexedOutputPipe) checkSubmissionOrder(channelName, start, end string) error {
	o.submittedWatermarksMu.Lock()
	defer o.submittedWatermarksMu.Unlock()
	if o.submittedWatermarks == nil {
		// Constructed directly (as several unit tests do) rather than via
		// newSnowflakeStreamerPipe, which would otherwise have initialized
		// this.
		o.submittedWatermarks = map[string]string{}
	}
	if prev, ok := o.submittedWatermarks[channelName]; ok && v2.CompareOffsetTokens(end, prev) < 0 {
		o.metrics.ReportOutOfOrderSubmission()
		return fmt.Errorf(
			"channel %q received offset tokens %s..%s after this process had already submitted a batch up to %s -- "+
				"messages are arriving out of order for this channel, which breaks offset_token's exactly-once guarantee "+
				"(see the channel_name field's docs: a channel must be written to by only one ordered source, "+
				"with at most one batch in flight to it at a time). This will not resolve itself on retry: fix "+
				"channel_name (give each independent source its own channel) or max_in_flight (cap it at 1 for a "+
				"shared channel) rather than relying on redelivery",
			channelName, start, end, prev)
	}
	if prev, ok := o.submittedWatermarks[channelName]; !ok || v2.CompareOffsetTokens(end, prev) > 0 {
		o.submittedWatermarks[channelName] = end
	}
	return nil
}

func (o *snowpipeIndexedOutputPipe) openChannel(ctx context.Context, name string) (v2.IngestChannel, error) {
	o.logger.Debugf("opening snowflake streaming channel for `%s.%s`: %s", o.db, o.schema, name)
	return o.openChannelFn(ctx, name)
}

// logReopenFailure warns that reopening name failed after an AppendRows or
// WaitUntilCommitted error, distinguishing the one reopenErr worth calling
// out specifically: ERR_CHANNEL_HAS_UNCOMMITTED_DATA means the channel still
// has appended-but-uncommitted data from before the failure, which is why
// OpenChannel refuses to force past it rather than risk racing the pending
// commit (see IsUncommittedDataConflict's doc comment) -- an operator
// reading this log needs to know that's the specific reason reopening
// didn't happen, not just that it didn't.
func (o *snowpipeIndexedOutputPipe) logReopenFailure(name string, reopenErr error) {
	if v2.IsUncommittedDataConflict(reopenErr) {
		o.logger.Warnf("unable to reopen channel %q: it still has uncommitted data from before the failure, so Snowflake refused the reopen rather than risk racing the pending commit: %v", name, reopenErr)
		return
	}
	o.logger.Warnf("unable to reopen channel %q after failure: %v", name, reopenErr)
}

func (*snowpipeIndexedOutputPipe) Connect(context.Context) error {
	return nil
}

// channelGroup is one channel's share of a WriteBatch call's messages: the
// resolved channel name, plus those messages in their original relative
// order.
type channelGroup struct {
	name  string
	batch service.MessageBatch
}

// groupMessagesByChannel resolves exec (built from the output's channel_name
// mapping) against every message in batch and buckets them into
// channelGroups keyed by resolved channel name -- rather than resolving the
// channel name from message 0 only and applying it to the whole batch, which
// silently mishandles a batch spanning more than one channel. Groups are
// returned in first-appearance order; each group preserves the relative
// order of its own messages from batch, since preprocessForExactlyOncePipe's
// dedup/ordering pass assumes increasing offset-token order within a
// channel.
func groupMessagesByChannel(exec *service.MessageBatchInterpolationExecutor, batch service.MessageBatch) ([]channelGroup, error) {
	groupIndex := make(map[string]int, 1)
	groups := make([]channelGroup, 0, 1)
	for i, msg := range batch {
		name, err := exec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("error executing %s: %w", ssopFieldChannelName, err)
		}
		idx, exists := groupIndex[name]
		if !exists {
			idx = len(groups)
			groupIndex[name] = idx
			groups = append(groups, channelGroup{name: name})
		}
		groups[idx].batch = append(groups[idx].batch, msg)
	}
	return groups, nil
}

func (o *snowpipeIndexedOutputPipe) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	exec := batch.InterpolationExecutor(o.channelName)
	groups, err := groupMessagesByChannel(exec, batch)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := o.writeChannelGroup(ctx, group); err != nil {
			// Returning on the first group's error, rather than continuing
			// on to the rest, is correct here and not just simpler: Benthos
			// treats a WriteBatch error as a whole-batch nack/retry, so on
			// retry every group -- including ones that already succeeded --
			// flows through writeChannelGroup again. A group that already
			// committed either dedupes back down to nothing (offset_token
			// set: the offset-token filter drops rows at or below the
			// channel's already-advanced committed token) or is safely
			// re-sent (offset_token unset: the same at-least-once tradeoff a
			// single-channel batch already had, now correctly scoped
			// per-channel instead of accidentally applied to the whole
			// batch). No new failure mode is introduced by grouping, so
			// aggregating the remaining groups' errors here would not
			// change what the retry does with them.
			return err
		}
	}
	return nil
}

// writeChannelGroup runs the append/commit cycle for one channel's share of
// a WriteBatch call: acquire the channel, optionally dedup/order via
// preprocessForExactlyOncePipe, append the rows, wait for commit, report
// rejected-row metrics, then release the channel back to the pool.
func (o *snowpipeIndexedOutputPipe) writeChannelGroup(ctx context.Context, group channelGroup) error {
	// Acquire keys the pool by group.name; every Release below keys by
	// channel.Name() instead. These only agree because v2.Client.OpenChannel
	// always constructs Channel.name == the name it was given, so keep that
	// invariant in mind before changing either side of this pairing.
	channel, err := o.channelPool.Acquire(ctx, group.name)
	if err != nil {
		return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
	}
	batch := group.batch
	var startOffsetToken, endOffsetToken string
	if o.offsetToken != nil {
		// origFirst/origLast are this call's own token range, before
		// preprocessForExactlyOncePipe's dedup filter runs -- checked and
		// recorded against this process's own submission history first, so
		// an ordering violation is caught even when the filter below would
		// otherwise treat the whole group as an unremarkable already-landed
		// duplicate. See checkSubmissionOrder's own doc comment.
		var origFirst, origLast string
		exec := batch.InterpolationExecutor(o.offsetToken)
		if origFirst, err = requireOffsetToken(exec, 0); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		if origLast, err = requireOffsetToken(exec, len(batch)-1); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		if err = o.checkSubmissionOrder(channel.Name(), origFirst, origLast); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		batch, startOffsetToken, endOffsetToken, err = preprocessForExactlyOncePipe(ctx, channel, o.offsetToken, batch)
		if err != nil || len(batch) == 0 {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
	} else {
		// No offsetToken means no dedup pass, but WaitUntilCommitted below
		// still needs a non-empty, call-unique token to poll for -- see
		// noDedupCommitToken's doc comment on why a fixed value (including
		// empty) can't drive that wait correctly.
		token := strconv.FormatInt(o.noDedupCommitToken.Add(1), 10)
		startOffsetToken, endOffsetToken = token, token
	}
	// When o.offsetToken is nil (offset_token isn't configured), dedup and
	// ordering are skipped entirely: every row in the group is appended as
	// received, tagged only with the synthetic per-call token generated
	// above. That's genuine at-least-once delivery, not an error -- see the
	// offset_token field's Description.
	o.logger.Debugf("inserting rows using channel %s at offset tokens: start=%s end=%s", channel.Name(), startOffsetToken, endOffsetToken)
	rows := make([][]byte, len(batch))
	for i, msg := range batch {
		row, err := msg.AsBytes()
		if err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return fmt.Errorf("unable to serialize message: %w", err)
		}
		rows[i] = row
	}
	appendStart := time.Now()
	if err := channel.AppendRows(ctx, rows, startOffsetToken, endOffsetToken); err != nil {
		if v2.IsBackpressure(err) {
			// Backpressure (HTTP 429/503, or one of the Kafka Connector's
			// four retryable SFException codes) does not invalidate the
			// channel -- unlike other append failures, the channel remains
			// open and usable, so it goes back to the pool unchanged rather
			// than being reopened. Reopening here would be pure overhead on
			// the control plane precisely when it's already saturated.
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		reopened, reopenErr := o.openChannel(ctx, channel.Name())
		if reopenErr == nil {
			// Releasing reopened under channel.Name() (not reopened.Name())
			// relies on the same pool-key invariant noted at Acquire above.
			o.channelPool.Release(channel.Name(), reopened)
		} else {
			o.logReopenFailure(channel.Name(), reopenErr)
			// Keep around the same channel so retry opening later
			o.channelPool.Release(channel.Name(), channel)
		}
		return err
	}
	appendDuration := time.Since(appendStart)
	o.logger.Debugf("done inserting %d rows using channel %s in %s", len(batch), channel.Name(), appendDuration)
	commitStart := time.Now()
	if err := channel.WaitUntilCommitted(ctx, endOffsetToken, o.commitTimeout); err != nil {
		reopened, reopenErr := o.openChannel(ctx, channel.Name())
		if reopenErr == nil {
			// Same pool-key invariant as the AppendRows failure path above.
			o.channelPool.Release(channel.Name(), reopened)
		} else {
			o.logReopenFailure(channel.Name(), reopenErr)
			// Keep around the same channel so retry opening later
			o.channelPool.Release(channel.Name(), channel)
		}
		return err
	}
	commitDuration := time.Since(commitStart)
	o.logger.Debugf("batch of %d rows committed using channel %s in %s", len(batch), channel.Name(), commitDuration)
	// rejected is the delta since the last time this channel reported a
	// positive delta (see streamingv2.IngestChannel.RowsRejected), i.e. rows
	// Snowflake counted as rejected because of (or since) this batch.
	// RowsErrorCount is the channel's absolute lifetime total, read after
	// RowsRejected so "total" and "initial" (total minus this delta) line
	// up with the delta just reported -- mirroring the Kafka Connector's
	// SnowpipeStreamingPartitionChannel, which snapshots initialErrorCount
	// at open and advances it the same way, before branching on
	// errors.tolerance.
	if rejected := channel.RowsRejected(); rejected > 0 {
		total := channel.RowsErrorCount()
		initial := total - rejected
		// o.metrics.ReportRowsRejected fires unconditionally here, before the
		// tolerate/fail branch below, so the snowflake_rows_error_count
		// metric is incremented by the same delta in both modes.
		o.metrics.ReportRowsRejected(rejected)
		msg := fmt.Sprintf("channel %q has %d new errors (total: %d, initial: %d). Last error message: %s",
			channel.Name(), rejected, total, initial, channel.LastErrorMessage())
		if o.tolerateRowErrors {
			o.logger.Warnf("%s", msg)
		} else {
			// The channel itself is still healthy (no append/commit error),
			// so release it for reuse rather than forcing a reopen -- only
			// this batch fails.
			o.channelPool.Release(channel.Name(), channel)
			return fmt.Errorf("%s", msg)
		}
	} else if total := channel.RowsErrorCount(); total > 0 {
		// Pre-existing errors this batch didn't add to: the Kafka
		// Connector's equivalent branch (currentErrorCount > 0 but no new
		// errors) logs at debug only, never warn/error, since a channel
		// reopened after an earlier rejection would otherwise refuse to
		// start (or every later batch would otherwise re-warn on) a
		// historical error nothing in this batch caused.
		o.logger.Debugf("channel %s has %d pre-existing row error(s) not from this batch: %s", channel.Name(), total, channel.LastErrorMessage())
	}
	o.metrics.Report(appendDuration, commitDuration)
	o.channelPool.Release(channel.Name(), channel)
	return nil
}

func (o *snowpipeIndexedOutputPipe) Close(context.Context) error {
	o.channelPool.Reset()
	return nil
}

// preprocessForExactlyOncePipe filters batch down to messages whose
// interpolated offset token is newer than the channel's latest committed
// offset token, and returns the raw offset tokens of the first and last
// message in the (possibly filtered) batch, for the caller to pass to
// AppendRows / WaitUntilCommitted.
//
// Tokens are compared with v2.CompareOffsetTokens, not a plain string
// comparison: Kafka offsets are decimal integers, and "10" < "9"
// lexicographically, so a string comparison silently reintroduces
// duplicates the moment a partition's offset reaches double digits.
//
// It's assumed the batch is already in increasing offset-token order (see
// the offset_token field's ordering note); an out-of-order batch will have
// its later, lower-offset rows dropped as if they were duplicates.
func preprocessForExactlyOncePipe(
	ctx context.Context,
	channel v2.IngestChannel,
	offsetTokenMapping *service.InterpolatedString,
	batch service.MessageBatch,
) (service.MessageBatch, string, string, error) {
	latest, err := channel.LatestOffsetToken(ctx)
	if err != nil {
		return nil, "", "", err
	}
	exec := batch.InterpolationExecutor(offsetTokenMapping)
	firstRawToken, err := requireOffsetToken(exec, 0)
	if err != nil {
		return nil, "", "", err
	}
	lastRawToken, err := requireOffsetToken(exec, len(batch)-1)
	if err != nil {
		return nil, "", "", err
	}
	// fast path: nothing committed yet, or the whole batch is ahead -> no filtering needed
	if latest == "" || v2.CompareOffsetTokens(firstRawToken, latest) > 0 {
		return batch, firstRawToken, lastRawToken, nil
	}
	// We need to filter out data that is too old.
	filteredBatch := make(service.MessageBatch, 0, len(batch))
	for i := range batch {
		rawToken, err := requireOffsetToken(exec, i)
		if err != nil {
			return nil, "", "", err
		}
		// per-row filter: at or below the committed token -> already landed, drop it
		if v2.CompareOffsetTokens(rawToken, latest) <= 0 {
			continue
		}
		filteredBatch = append(filteredBatch, batch[i])
	}
	if len(filteredBatch) == 0 {
		return filteredBatch, "", "", nil
	}
	// This is a lazy way to compute the bounds, but filtering should be a rare operation.
	return preprocessForExactlyOncePipe(ctx, channel, offsetTokenMapping, filteredBatch)
}

// requireOffsetToken interpolates the offset_token expression for message i
// and fails loudly if it resolves to an empty string, rather than letting an
// empty token flow into the comparisons above. An empty token is exactly
// the sentinel channel.LatestOffsetToken uses for "nothing committed yet",
// so silently accepting one would make a real row indistinguishable from
// that sentinel. Only called when offset_token is explicitly configured
// (see writeChannelGroup): there's no default expression to collide with
// here anymore, but the check stays unconditional regardless, since the
// collision is the same problem for any offset_token value the operator
// writes. Never substitute a synthetic or counter-derived token here: that
// would silently disable exactly-once instead of failing loudly -- contrast
// noDedupCommitToken, which only ever runs when the operator has not
// configured offset_token at all, and so is never a substitute for a token
// this function was expecting to validate.
//
// A missing metadata reference (e.g. @kafka_offset on a non-Kafka input,
// still a common expression to reach for since it's shown in this field's
// docs as one example) does not interpolate to "": Bloblang stringifies a
// null query result as the literal string "null" (value.IToBytes/IToString
// in the benthos SDK), so that case is checked for explicitly too -- an
// unconditional "" check alone would let every row silently share the token
// "null" instead of failing.
func requireOffsetToken(exec *service.MessageBatchInterpolationExecutor, index int) (string, error) {
	token, err := exec.TryString(index)
	if err != nil {
		return "", err
	}
	if token == "" || token == "null" {
		return "", fmt.Errorf("%s interpolated to %q for message %d: exactly-once delivery requires a non-empty offset token for every message", ssopFieldOffsetToken, token, index)
	}
	return token, nil
}
