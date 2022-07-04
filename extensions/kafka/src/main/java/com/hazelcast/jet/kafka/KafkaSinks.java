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

package com.hazelcast.jet.kafka;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;
import java.util.Properties;

import static com.hazelcast.jet.kafka.KafkaProcessors.writeKafkaP;

/**
 * Contains factory methods for Apache Kafka sinks.
 *
 * @since Jet 3.0
 */
public final class KafkaSinks {

    private KafkaSinks() {
    }

    /**
     * Returns a sink that publishes messages to Apache Kafka topic(s). It
     * transforms each received item to a {@code ProducerRecord} using the
     * supplied mapping function.
     * <p>
     * The sink creates a single {@code KafkaProducer} per processor using
     * the supplied {@code properties}.
     * <p>
     * The behavior depends on the job's processing guarantee:
     * <ul>
     *     <li><em>EXACTLY_ONCE:</em> the sink will use Kafka transactions to
     *     commit the messages. Transactions are committed after a snapshot is
     *     completed. This increases the latency of the messages because they
     *     are only visible to consumers after they are committed and slightly
     *     reduces the throughput because no messages are sent between the
     *     snapshot phases.
     *     <p>
     *     When using transactions pay attention to your {@code
     *     transaction.timeout.ms} config property. It limits the entire
     *     duration of the transaction since it is begun, not just inactivity
     *     timeout. It must not be smaller than your snapshot interval,
     *     otherwise the Kafka broker will roll the transaction back before Jet
     *     is done with it. Also it should be large enough so that Jet has time
     *     to restart after a failure: a member can crash just before it's
     *     about to commit, and Jet will attempt to commit the transaction
     *     after the restart, but the transaction must be still waiting in the
     *     broker. The default in Kafka 2.4 is 1 minute.
     *     <p>
     *     Also keep in mind the consumers need to use {@code
     *     isolation.level=read_committed}, which is not the default. Otherwise
     *     the consumers will see duplicate messages.
     *
     *     <li><em>AT_LEAST_ONCE:</em> messages are committed immediately, the
     *     sink ensure that all async operations are done at 1st snapshot
     *     phase. This ensures that each message is written if the job fails,
     *     but might be written again after the job restarts.
     * </ul>
     *
     * If you want to avoid the overhead of transactions, you can reduce the
     * guarantee just for the sink. To do so, use the builder version and call
     * {@link Builder#exactlyOnce(boolean) exactlyOnce(false)} on the builder.
     * <p>
     * IO failures are generally handled by Kafka producer and do not cause the
     * processor to fail. Refer to Kafka documentation for details.
     * <p>
     * The default local parallelism for this processor is 1.
     *
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param toRecordFn   function that extracts the key from the stream item
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     */
    @Nonnull
    public static <E, K, V> Sink<E> kafka(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super E, ProducerRecord<K, V>> toRecordFn
    ) {
        return Sinks.fromProcessor("kafkaSink", writeKafkaP(properties, toRecordFn, true));
    }

    /**
     * Convenience for {@link #kafka(Properties, FunctionEx)} which creates
     * a {@code ProducerRecord} using the given topic and the given key and value
     * mapping functions
     *
     * @param <E> type of stream item
     * @param <K> type of the key published to Kafka
     * @param <V> type of the value published to Kafka
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param topic          name of the Kafka topic to publish to
     * @param extractKeyFn   function that extracts the key from the stream item
     * @param extractValueFn function that extracts the value from the stream item
     */
    @Nonnull
    public static <E, K, V> Sink<E> kafka(
            @Nonnull Properties properties,
            @Nonnull String topic,
            @Nonnull FunctionEx<? super E, K> extractKeyFn,
            @Nonnull FunctionEx<? super E, V> extractValueFn
    ) {
        return Sinks.fromProcessor("kafkaSink(" + topic + ")",
                writeKafkaP(properties, topic, extractKeyFn, extractValueFn, true));
    }

    /**
     * Convenience for {@link #kafka(Properties, String, FunctionEx, FunctionEx)}
     * which expects {@code Map.Entry<K, V>} as input and extracts its key and value
     * parts to be published to Kafka.
     *
     * @param <K>        type of the key published to Kafka
     * @param <V>        type of the value published to Kafka
     * @param properties producer properties which should contain broker
     *                   address and key/value serializers
     * @param topic      Kafka topic name to publish to
     */
    @Nonnull
    public static <K, V> Sink<Entry<K, V>> kafka(@Nonnull Properties properties, @Nonnull String topic) {
        return kafka(properties, topic, Entry::getKey, Entry::getValue);
    }

    /**
     * Returns a builder object that you can use to create an Apache Kafka
     * pipeline sink.
     * <p>
     * The sink creates a single {@code KafkaProducer} per processor using
     * the supplied {@code properties}.
     * <p>
     * The behavior depends on the job's processing guarantee:
     * <ul>
     *     <li><em>EXACTLY_ONCE:</em> the sink will use Kafka transactions to
     *     commit the messages. This brings some overhead on the broker side,
     *     slight throughput reduction (we don't send messages between snapshot
     *     phases) and, most importantly, increases the latency of the messages
     *     because they are only visible to consumers after they are committed.
     *     <p>
     *     When using transactions pay attention to your {@code
     *     transaction.timeout.ms} config property. It limits the entire
     *     duration of the transaction since it is begun, not just inactivity
     *     timeout. It must not be smaller than your snapshot interval,
     *     otherwise the Kafka broker will roll the transaction back before Jet
     *     is done with it. Also it should be large enough so that Jet has time
     *     to restart after a failure: a member can crash just before it's
     *     about to commit, and Jet will attempt to commit the transaction
     *     after the restart, but the transaction must be still waiting in the
     *     broker. The default in Kafka 2.4 is 1 minute.
     *
     *     <li><em>AT_LEAST_ONCE:</em> messages are committed immediately, the
     *     sink ensure that all async operations are done at 1st snapshot
     *     phase. This ensures that each message is written if the job fails,
     *     but might be written again after the job restarts.
     * </ul>
     *
     * If you want to avoid the overhead of transactions, you can reduce the
     * guarantee just for the sink by calling {@link
     * Builder#exactlyOnce(boolean) exactlyOnce(false)} on the returned builder.
     * <p>
     * IO failures are generally handled by Kafka producer and do not cause the
     * processor to fail. Refer to Kafka documentation for details.
     * <p>
     * Default local parallelism for this processor is 1.
     *
     * @param properties     producer properties which should contain broker
     *                       address and key/value serializers
     * @param <E> type of stream item
     */
    @Nonnull
    public static <E> Builder<E> kafka(@Nonnull Properties properties) {
        return new Builder<>(properties);
    }

    /**
     * A builder for Kafka sink.
     *
     * @param <E> type of stream item
     */
    public static final class Builder<E> {

        private final Properties properties;
        private FunctionEx<? super E, ? extends ProducerRecord<Object, Object>> toRecordFn;
        private String topic;
        private FunctionEx<? super E, ?> extractKeyFn;
        private FunctionEx<? super E, ?> extractValueFn;
        private boolean exactlyOnce = true;

        private Builder(Properties properties) {
            this.properties = properties;
        }

        /**
         * Sets the topic to write the messages to, if you write all messages
         * to a single topic. If you write each message to a different topic,
         * use {@link #toRecordFn(FunctionEx)}. If you call this method, you
         * can't call the {@code toRecordFn()} method.
         *
         * @param topic the topic name
         * @return this instance for fluent API
         */
        @Nonnull
        public Builder<E> topic(String topic) {
            if (toRecordFn != null) {
                throw new IllegalArgumentException("toRecordFn already set, you can't use topic if it's set");
            }
            this.topic = topic;
            return this;
        }

        /**
         * Sets the function to extract the key from the stream items. You
         * can't use this method in combination with {@link
         * #toRecordFn(FunctionEx)}.
         * <p>
         * The default is to use {@code null} key.
         *
         * @param extractKeyFn a function to extract the key from the stream item
         * @return this instance for fluent API
         */
        @Nonnull
        public Builder<E> extractKeyFn(@Nonnull FunctionEx<? super E, ?> extractKeyFn) {
            if (toRecordFn != null) {
                throw new IllegalArgumentException("toRecordFn already set, you can't use extractKeyFn if it's set");
            }
            this.extractKeyFn = extractKeyFn;
            return this;
        }

        /**
         * Sets the function to extract the value from the stream items. You
         * can't use this method in combination with {@link
         * #toRecordFn(FunctionEx)}.
         * <p>
         * The default is to use the input item directly.
         *
         * @param extractValueFn a function to extract the value from the stream item
         * @return this instance for fluent API
         */
        @Nonnull
        public Builder<E> extractValueFn(@Nonnull FunctionEx<? super E, ?> extractValueFn) {
            if (toRecordFn != null) {
                throw new IllegalArgumentException("toRecordFn already set, you can't use extractValueFn if it's set");
            }
            this.extractValueFn = extractValueFn;
            return this;
        }

        /**
         * Sets the function to convert stream items into Kafka's {@code
         * ProducerRecord}. This method allows you to specify all aspects of
         * the record (different topic for each message, partition,
         * timestamp...). If you use this method, you can't use {@link
         * #topic(String)}, {@link #extractKeyFn(FunctionEx)} or {@link
         * #extractValueFn(FunctionEx)}.
         *
         * @param toRecordFn a function to convert stream items into Kafka's
         *      {@code ProducerRecord}
         * @return this instance for fluent API
         */
        @SuppressWarnings("unchecked")
        @Nonnull
        public Builder<E> toRecordFn(@Nullable FunctionEx<? super E, ? extends ProducerRecord<?, ?>> toRecordFn) {
            if (topic != null || extractKeyFn != null || extractValueFn != null) {
                throw new IllegalArgumentException("topic, extractKeyFn or extractValueFn are already set, you can't use" +
                        " toRecordFn along with them");
            }
            this.toRecordFn = (FunctionEx<? super E, ? extends ProducerRecord<Object, Object>>) toRecordFn;
            return this;
        }

        /**
         * Enables or disables the exactly-once behavior of the sink using
         * two-phase commit of state snapshots. If enabled, the {@linkplain
         * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
         * guarantee} of the job must be set to {@linkplain
         * ProcessingGuarantee#EXACTLY_ONCE exactly-once}, otherwise the sink's
         * guarantee will match that of the job. In other words, sink's
         * guarantee cannot be higher than job's, but can be lower to avoid the
         * additional overhead.
         * <p>
         * See {@link #kafka(Properties)} for more information.
         * <p>
         * The default value is true.
         *
         * @param enable If true, sink's guarantee will match the job
         *      guarantee. If false, sink's guarantee will be at-least-once
         *      even if job's is exactly-once
         * @return this instance for fluent API
         */
        @Nonnull
        public Builder<E> exactlyOnce(boolean enable) {
            exactlyOnce = enable;
            return this;
        }

        /**
         * Builds the Sink object that you pass to the {@link
         * GeneralStage#writeTo(Sink)} method.
         */
        @Nonnull
        public Sink<E> build() {
            if ((extractValueFn != null || extractKeyFn != null) && topic == null) {
                throw new IllegalArgumentException("if `extractKeyFn` or `extractValueFn` are set, `topic` must be set " +
                        "too");
            }
            if (topic == null && toRecordFn == null) {
                throw new IllegalArgumentException("either `topic` or `toRecordFn` must be set");
            }
            if (topic != null) {
                FunctionEx<? super E, ?> extractKeyFn1 = extractKeyFn != null ? extractKeyFn : t -> null;
                FunctionEx<? super E, ?> extractValueFn1 = extractValueFn != null ? extractValueFn : t -> t;
                return Sinks.fromProcessor("kafkaSink(" + topic + ")",
                        writeKafkaP(properties, topic, extractKeyFn1, extractValueFn1, exactlyOnce));
            } else {
                ProcessorMetaSupplier metaSupplier = writeKafkaP(properties, toRecordFn, exactlyOnce);
                return Sinks.fromProcessor("kafkaSink", metaSupplier);
            }
        }
    }
}
