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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.kinesis.impl.sink.KinesisSinkPSupplier;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.DISTRIBUTED_PARTITIONED;

/**
 * Contains factory methods for creating Amazon Kinesis Data Streams
 * (KDS) sinks.
 *
 * @since Jet 4.4
 */
public final class KinesisSinks {

    /**
     * Kinesis partition keys are Unicode strings, with a maximum length
     * limit of 256 characters for each key.
     */
    public static final int MAXIMUM_KEY_LENGTH = 256;

    /**
     * The length of a record's data blob (byte array length), plus the
     * record's key size (no. of Unicode characters in the key), must
     * not be larger than 1MiB.
     */
    public static final int MAX_RECORD_SIZE = 1024 * 1024;

    /**
     * One of the metrics exposed by the sink used to monitor the
     * current sending batch size. The batch size is computed based on
     * the number of shards in the stream. The more shards, the bigger
     * the batch size, the more data the stream can ingest. The maximum
     * value is 500, the limit imposed by Kinesis.
     */
    public static final String BATCH_SIZE_METRIC = "batchSize";

    /**
     * One of the metrics exposed by the sink used to monitor the
     * current sleep delay between consecutive sends (in milliseconds).
     * When the flow control mechanism is deactivated, the value should
     * always be zero. If flow control kicks in, the value keeps
     * increasing until shard ingestion rates are no longer tripped,
     * then stabilizes, then slowly decreases back to zero (if the data
     * rate was just a spike and is not sustained).
     */
    public static final String THROTTLING_SLEEP_METRIC = "throttlingSleep";

    private KinesisSinks() {
    }

    /**
     * Initiates the building of a sink that publishes messages into
     * Amazon Kinesis Data Streams (KDS).
     * <p>
     * The basic unit of data stored in KDS is the <em>record</em>. A
     * record is composed of a sequence number, partition key, and data
     * blob. KDS assigns the sequence numbers on ingestion; the other
     * two have to be specified by the sink.
     * <p>
     * The key function we provide specifies how to assign <em>partition
     * keys</em> to the items to be published. The partition keys have
     * the role of grouping related records so that Kinesis can handle
     * them accordingly. Partition keys are Unicode strings with a
     * maximum length of 256 characters.
     * <p>
     * The value function we provide specifies how to extract the useful
     * data content from the items to be published. It's basically
     * serialization of the messages (Kinesis handles neither
     * serialization nor deserialization internally). The length of the
     * resulting byte array, plus the length of the partition key, can't
     * be longer than 1MiB.
     * <p>
     * The sink is <em>distributed</em> Each instance handles a subset
     * of possible partition keys. In Jet terms, the sink processors'
     * input edges are distributed and partitioned by the same key
     * function we provide for computing the partition keys. As a
     * result, each item with the same partition key will end up in the
     * same distributed sink instance.
     * <p>
     * The sink can be used in both streaming and batching pipelines.
     * <p>
     * The only processing guarantee the sink can support is
     * <em>at-least-once</em>. This is caused by the lack of transaction
     * support in Kinesis (can't write data into it with transactional
     * guarantees) and the AWS SDK occasionally causing data duplication
     * on its own (@see <a
     * href="https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html">
     * Producer Retries</a> in the documentation).
     * <p>
     * The sink preserves the ordering of items for the same partition
     * key, as long as the stream's ingestion rate is not tripped.
     * Ingestion rates in Kinesis are specified on a per <em>shard</em>
     * basis, which is the base throughput unit. One shard provides a
     * capacity of 1MiB/sec data input and 2MiB/sec data output. One
     * shard can support up to 1000 record publications per second. The
     * owner of the Kinesis stream specifies the number of shards needed
     * when creating the stream. It can be changed later without
     * stopping the data flow.
     * <p>
     * If the sinks attempt to write more into a shard than allowed,
     * some records will be rejected. This rejection breaks the ordering
     * because the sinks write data in batches, and the shards don't
     * just reject entire batches but random records from them. What's
     * rejected can (and is) retried, but the batch's original ordering
     * can't be preserved.
     * <p>
     * The sink cannot avoid rejections entirely because multiple
     * instances of it write into the same shard, and coordinating an
     * aggregated rate among them is not something currently possible in
     * Jet.
     * <p>
     * The sink has a flow control mechanism, which tries to minimize
     * the amount of ingestion rate tripping, when it starts happening,
     * by reducing the send batch size and introducing adaptive sleep
     * delays between consecutive sends. However, the only sure way to
     * avoid the problem is having enough shards and a good spread of
     * partition keys (partition keys are assigned to shard based on an
     * MD5 hash function; each shard owns a partition of the hash
     * space).
     * <p>
     * The sink exposes metrics that can be used to monitor the flow
     * control mechanism. One of them is the
     * {@link KinesisSinks#BATCH_SIZE_METRIC send batch size}; the other
     * one is the
     * {@link KinesisSinks#THROTTLING_SLEEP_METRIC sleep delay between consecutive sends}.
     *
     * @param stream  name of the Kinesis stream being written into
     * @param keyFn   function for computing partition keys
     * @param valueFn function for computing serialized message data
     * content
     * @return fluent builder that can be used to set properties and
     * also to construct the sink once configuration is done
     */
    @Nonnull
    public static <T> Builder<T> kinesis(
            @Nonnull String stream,
            @Nonnull FunctionEx<T, String> keyFn,
            @Nonnull FunctionEx<T, byte[]> valueFn
    ) {
        return new Builder<>(stream, keyFn, valueFn);
    }

    /**
     * Convenience method for a specific type of sink, one that ingests
     * items of type {@code Map.Entry<String, byte[]>} and assumes that
     * the entries key is the partition key and the entries value is the
     * record data blob. No explicit key nor value function needs to be
     * specified.
     */
    @Nonnull
    public static Builder<Map.Entry<String, byte[]>> kinesis(@Nonnull String stream) {
        return new Builder<>(stream, Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Fluent builder for constructing the Kinesis sink and setting its
     * configuration parameters.
     * @param <T> Type of items ingested by this sink
     */
    public static final class Builder<T> {

        private static final long INITIAL_RETRY_TIMEOUT_MS = 100L;
        private static final long MAXIMUM_RETRY_TIMEOUT_MS = 3_000L;
        private static final double EXPONENTIAL_BACKOFF_MULTIPLIER = 2.0;

        private static final RetryStrategy DEFAULT_RETRY_STRATEGY = RetryStrategies.custom()
                .intervalFunction(IntervalFunction.exponentialBackoffWithCap(
                        INITIAL_RETRY_TIMEOUT_MS, EXPONENTIAL_BACKOFF_MULTIPLIER, MAXIMUM_RETRY_TIMEOUT_MS))
                .build();

        @Nonnull
        private final String stream;
        @Nonnull
        private final FunctionEx<T, String> keyFn;
        @Nonnull
        private final FunctionEx<T, byte[]> valueFn;
        @Nonnull
        private final AwsConfig awsConfig = new AwsConfig();
        @Nonnull
        private RetryStrategy retryStrategy = DEFAULT_RETRY_STRATEGY;

        /**
         * Fluent builder for constructing the Kinesis sink and setting
         * its configuration parameters.
         */
        private Builder(
                @Nonnull String stream,
                @Nonnull FunctionEx<T, String> keyFn,
                @Nonnull FunctionEx<T, byte[]> valueFn
        ) {
            this.stream = stream;
            this.keyFn = keyFn;
            this.valueFn = valueFn;
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
         * Construct the sink based on the options provided so far.
         */
        @Nonnull
        public Sink<T> build() {
            String name = "Kinesis Sink (" + stream + ")";
            KinesisSinkPSupplier<T> supplier =
                    new KinesisSinkPSupplier<>(awsConfig, stream, keyFn, valueFn, retryStrategy);
            return new SinkImpl<>(name, ProcessorMetaSupplier.of(supplier), DISTRIBUTED_PARTITIONED, keyFn);
        }
    }

}
