/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.source.InitialShardIterators;
import com.hazelcast.jet.kinesis.impl.source.KinesisSourcePMetaSupplier;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.Util.entry;

/**
 * Contains factory methods for creating Amazon Kinesis Data Streams
 * (KDS) sources.
 *
 * @since Jet 4.4
 */
public final class KinesisSources {

    /**
     * Name of the metric exposed by the source, used to monitor if reading a
     * specific shard is delayed or not.  The value represents the number of
     * milliseconds the last read record is behind the tip of the stream.
     */
    public static final String MILLIS_BEHIND_LATEST_METRIC = "millisBehindLatest";

    private KinesisSources() {
    }

    /**
     * Initiates the building of a streaming source that consumes a
     * Kinesis data stream and emits {@code Map.Entry<String, byte[]>}
     * items.
     * <p>
     * Each emitted item represents a Kinesis <em>record</em>, the basic
     * unit of data stored in KDS. A record is composed of a sequence
     * number, partition key, and data blob. This source does not expose
     * the sequence numbers, uses them only internally, for replay
     * purposes.
     * <p>
     * The <em>partition key</em> is used to group related records and
     * route them inside Kinesis. Partition keys are Unicode strings
     * with a maximum length of 256 characters. They are specified by
     * the data producer while adding data to KDS.
     * <p>
     * The <em>data blob</em> of the record is provided in the form of a
     * {@code byte} array, in essence, serialized data (Kinesis doesn't
     * handle serialization internally). The maximum size of the data
     * blob is 1 MB.
     * <p>
     * The source is <em>distributed</em>. Each instance is consuming
     * data from zero, one or more Kinesis <em>shards</em>, the base
     * throughput units of KDS. One shard provides a capacity of 2MB/sec
     * data output. Items with the same partition key always come from
     * the same shard; one shard contains multiple partition keys. Items
     * coming from the same shard are ordered, and the source preserves
     * this order (except on resharding). The local parallelism of the
     * source is not defined, so it will depend on the number of cores
     * available.
     * <p>
     * If a processing guarantee is specified for the job, Jet will
     * periodically save the current shard offsets internally and then
     * replay from the saved offsets when the job is restarted. If no
     * processing guarantee is enabled, the source will start reading
     * from the oldest available data, determined by the KDS retention
     * period (defaults to 24 hours, can be as long as 365 days). This
     * replay capability makes the source suitable for pipelines with
     * both <em>at-least-once</em> and <em>exactly-once</em> processing
     * guarantees.
     * <p>
     * The source can provide native timestamps, in the sense that they
     * are read from KDS, but be aware that they are Kinesis ingestion
     * times, not event times in the strictest sense.
     * <p>
     * As stated before, the source preserves the ordering inside
     * shards. However, Kinesis supports resharding, which lets you
     * adjust the number of shards in your stream to adapt to changes in
     * data flow rate through the stream. When resharding happens, the
     * source cannot preserve the order among the last items of a shard
     * being destroyed and the first items of new shards being created.
     * Watermarks might also experience unexpected behaviour during resharding.
     * Best to do resharding when the data flow is stopped, if possible.
     * <p>
     * <strong>NOTE</strong>. In Kinesis terms, this source is a "shared
     * throughput consumer." This means that all the limitations on data
     * read, imposed by Kinesis (at most 5 read transaction per second,
     * at most 2MiB of data per second) apply not on a per-source basis
     * but for all sources at once. If you start only one pipeline with
     * a source for a Kinesis stream, then the source implementation
     * will assure that no limit is tripped. But if you run multiple
     * pipelines with <strong>sources for the same Kinesis
     * stream</strong>, these sources cannot coordinate the limits among
     * themselves. It will still work, but you will see occasional
     * warning messages in the logs about rates and limitations being
     * tripped.
     * <p>
     * The source provides a metric called
     * {@value MILLIS_BEHIND_LATEST_METRIC}, which specifies, for each
     * shard, the source is actively reading data from if there is any
     * delay in reading the data.
     *
     * @param stream name of the Kinesis stream being consumed by the
     *               source
     * @return fluent builder that can be used to set properties and
     * also to construct the source once configuration is done
     */
    @Nonnull
    public static Builder<Map.Entry<String, byte[]>> kinesis(@Nonnull String stream) {
        return new Builder<>(Objects.requireNonNull(stream));
    }

    /**
     * Fluent builder for constructing the Kinesis source and setting
     * its configuration parameters.
     * @param <T> Result type returned from the source.
     */
    public static final class Builder<T> {

        private static final long INITIAL_RETRY_TIMEOUT_MS = 100L;
        private static final double EXPONENTIAL_BACKOFF_MULTIPLIER = 2.0;
        private static final long MAXIMUM_RETRY_TIMEOUT_MS = 3_000L;

        private static final RetryStrategy DEFAULT_RETRY_STRATEGY = RetryStrategies.custom()
                .intervalFunction(IntervalFunction.exponentialBackoffWithCap(
                        INITIAL_RETRY_TIMEOUT_MS, EXPONENTIAL_BACKOFF_MULTIPLIER, MAXIMUM_RETRY_TIMEOUT_MS))
                .build();

        @Nonnull
        private final String stream;
        @Nonnull
        private final AwsConfig awsConfig = new AwsConfig();
        @Nonnull
        private RetryStrategy retryStrategy = DEFAULT_RETRY_STRATEGY;
        @Nonnull
        private final InitialShardIterators initialShardIterators = new InitialShardIterators();
        @Nonnull
        private BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn;

        private Builder(@Nonnull String stream) {
            this.stream = stream;
            // Don't use lambda here since serialization/deserialization of the lambda gets corrupted
            // after relocating aws classes
            this.projectionFn = new DefaultProjection<>();
        }

        /**
         * Specifies the AWS Kinesis endpoint (URL of the entry point
         * for the AWS web service) to connect to. The general syntax of
         * these endpoint URLs is
         * "{@code protocol://service-code.region-code.amazonaws.com}",
         * so for example, for Kinesis, for the {@code us-west-2}
         * region, we could have
         * "{@code https://dynamodb.us-west-2.amazonaws.com}". For local
         * testing, it might be "{@code http://localhost:4566}".
         * <p>
         * If not specified (or specified as {@code null}), the default
         * endpoint for the specified region will be used.
         */
        @Nonnull
        public Builder<T> withEndpoint(@Nullable String endpoint) {
            awsConfig.withEndpoint(endpoint);
            return this;
        }

        /**
         * Specifies the AWS Region (collection of AWS resources in a
         * geographic area) to connect to. Region names are of form
         * "{@code us-west-1}", "{@code eu-central-1}" and so on.
         * <p>
         * If not specified (or specified as {@code null}), the default
         * region set via external means will be used (either from your
         * local {@code .aws/config} file or the {@code AWS_REGION}
         * environment variable). If no such default is set, then
         * "{@code us-east-1}" will be used.
         */
        @Nonnull
        public Builder<T> withRegion(@Nullable String region) {
            awsConfig.withRegion(region);
            return this;
        }

        /**
         * Specifies the AWS credentials to use for authentication
         * purposes.
         * <p>
         * If not specified (or specified as {@code null}), then keys
         * specified via external means will be used. This can mean the
         * local {@code .aws/credentials} file or the
         * {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}
         * environmental variables.
         * <p>
         * Either both keys must be set to non-null values or neither.
         */
        @Nonnull
        public Builder<T> withCredentials(@Nullable String accessKey, @Nullable String secretKey) {
            awsConfig.withCredentials(accessKey, secretKey);
            return this;
        }

        /**
         * Specifies how the source should behave when reading data from
         * the stream fails. The default behavior retries the read
         * operation indefinitely, but after an exponentially increasing
         * delay, it starts with 100 milliseconds and doubles on each
         * subsequent failure. A successful read resets it. The delay is
         * capped at 3 seconds.
         */
        @Nonnull
        public Builder<T> withRetryStrategy(@Nonnull RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }

        /**
         * Specifies how initial reads of the shards should be done (see
         * <em>shardIteratorType</em> for available options). Initial
         * read means the moment when a pipeline initiates reading a
         * shard for the first time. If a pipeline's execution is being
         * resumed based on a snapshot and there is a saved read offset
         * for the shard then it is <em>NOT</em> considered an initial
         * read.
         * <p>
         * Each call of this method registers one rule which applies to
         * any number of shards, depending on the <em>shardIdRegExp</em>
         * matching the shard's id or not. The syntax of regular
         * expressions is as defined by {@code java.util.regex.Pattern}.
         * The method can be called any number of times, the rules will
         * be evaluated in the order they have been registered. Only the
         * first rule that matches a shard will be applied for that
         * shard.
         * <p>
         * Type of rules allowed, together with their meaning, are:
         * <ul>
         *     <li><b>AT_SEQUENCE_NUMBER</b>: start reading the shard
         *      at the record with the specified sequence number</li>
         *     <li><b>AFTER_SEQUENCE_NUMBER</b>: start reading the shard
         *      right after the record with the specified sequence number</li>
         *     <li><b>AT_TIMESTAMP</b>: start reading at the first record
         *      with a timestamp equal to or greater than the provided one</li>
         *     <li><b>TRIM_HORIZON</b>: start reading at the oldest
         *      record in the shard, ie. read all available records</li>
         *     <li><b>LATEST</b>: start reading just after the most
         *      recent record in the shard, ie. read only records that will
         *      be ingested later into the shard</li>
         * </ul>
         * <p>
         * Depending on the rule specified the optional
         * <em>parameter</em> must also be specified. For
         * <b>AT_TIMESTAMP</b> it needs to be a timestamp in the Unix
         * epoch date format, with millisecond precision. For
         * <b>AT_SEQUENCE_NUMBER</b> and <b>AFTER_SEQUENCE_NUMBER</b> it
         * must be a sequence number accepted by KDS. For other types it
         * must be left {@code null}.
         * <p>
         * <strong>NOTE:</strong> The <b>AT_TIMESTAMP</b> version, even though
         * supported by Kinesis, doesn't currently work properly. This is due
         * to be a bug in AWS SDK v1, used by the source. AWS SDK v2 fixes the
         * problem and hopefully this source will be switched to using that
         * in future Jet releases.
         */
        @Nonnull
        public Builder<T> withInitialShardIteratorRule(@Nonnull String shardIdRegExp,
                                                    @Nonnull String shardIteratorType,
                                                    @Nullable String parameter) {
            initialShardIterators.add(shardIdRegExp, shardIteratorType, parameter);
            return this;
        }

        /**
         * Specifies projection function, that will map input {@link Record}
         * and {@link com.amazonaws.services.kinesis.model.Shard} from which this record was read into user-defined type.
         *
         * If not provided, source will return {@code Map.Entry<String, byte[]>} with {@link Record#getPartitionKey()}
         * as key and {@link Record#getData()} as value.
         */
        @Nonnull
        @SuppressWarnings("unchecked") // here we base on Java's type erasure, that's why we use that casting
        public <T_NEW> Builder<T_NEW> withProjectionFn(@Nonnull BiFunctionEx<Record, Shard, T_NEW> projectionFn) {
            this.projectionFn = (BiFunctionEx<Record, Shard, T>) projectionFn;
            return (Builder<T_NEW>) this;
        }

        /**
         * Constructs the source based on the options provided so far.
         */
        @Nonnull
        public StreamSource<T> build() {
            String stream = this.stream;
            AwsConfig awsConfig = this.awsConfig;
            RetryStrategy retryStrategy = this.retryStrategy;
            InitialShardIterators initialShardIterators = this.initialShardIterators;
            BiFunctionEx<? super Record, ? super Shard, ? extends T> projectionFn = this.projectionFn;
            return Sources.streamFromProcessorWithWatermarks(
                    "Kinesis Source (" + stream + ")",
                    true,
                    eventTimePolicy -> new KinesisSourcePMetaSupplier<T>(awsConfig, stream, retryStrategy,
                            initialShardIterators, eventTimePolicy, projectionFn));
        }
    }

    private static class DefaultProjection<T> implements BiFunctionEx<Record, Shard, T> {

        private static final long serialVersionUID = 1L;

        @Override
        public T applyEx(Record record, Shard shard) {
            return (T) entry(record.getPartitionKey(), toArray(record));
        }

        private static byte[] toArray(Record record) {
            ByteBuffer buffer = record.getData();
            int position = buffer.position();
            int limit = buffer.limit();
            if (position == 0 && limit == buffer.capacity()) {
                return buffer.array();
            } else {
                return Arrays.copyOfRange(buffer.array(), position, limit);
            }
        }
    }
}
