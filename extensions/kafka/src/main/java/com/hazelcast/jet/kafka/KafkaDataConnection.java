/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionBase;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.kafka.impl.NonClosingKafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * {@link DataConnection} implementation for Kafka.
 * <p>
 * KafkaDataConnection is usable both for sources and sinks.
 * <p>
 * Sources obtain {@link KafkaConsumer} instances using {@link #newConsumer()}.
 * Because the KafkaConsumer is not thread-safe so the DataConnection is used only
 * to keep the connection metadata. A new instance is returned each time this
 * method is called.
 * <p>
 * Sinks obtain {@link KafkaProducer} instances using {@link #getProducer(String)}.
 * The producer instance may be either shared or single-use depending on the
 * {@link DataConnectionConfig#isShared()} setting. Shared producer instance is limited
 * to use with either no processing guarantees or at-least-once processing guarantees.
 * Use with exactly-once is not possible.
 * <p>
 * The properties must be configured for both consumers and producers.
 *
 * @since 5.3
 */
public class KafkaDataConnection extends DataConnectionBase {

    private static final Properties EMPTY_PROPERTIES = new Properties();
    private volatile ConcurrentMemoizingSupplier<NonClosingKafkaProducer<?, ?>> producerSupplier;

    /**
     * Create {@link KafkaDataConnection} based on given config
     */
    public KafkaDataConnection(@Nonnull DataConnectionConfig config) {
        super(config);

        producerSupplier = new ConcurrentMemoizingSupplier<>(() ->
                new NonClosingKafkaProducer<>(config.getProperties(), this::release));
    }

    @Nonnull
    @Override
    public Collection<DataConnectionResource> listResources() {
        try (AdminClient client = AdminClient.create(getConfig().getProperties())) {
            return client.listTopics().names().get()
                    .stream()
                    .sorted()
                    .map(n -> new DataConnectionResource("topic", n))
                    .collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            throw new HazelcastException("Could not get list of topics for DataConnection " + getConfig().getName(), e);
        }
    }

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return Collections.singleton("Topic");
    }

    /**
     * Creates new instance of {@link KafkaConsumer} based on the DataConnection
     * configuration.
     * Always creates a new instance of the consumer because
     * {@link KafkaConsumer} is not thread-safe.
     * The caller is responsible for closing the consumer instance.
     *
     * @return consumer instance
     */
    @Nonnull
    public <K, V> Consumer<K, V> newConsumer() {
        return newConsumer(EMPTY_PROPERTIES);
    }

    /**
     * Creates new instance of {@link KafkaConsumer} based on the DataConnection
     * configuration and given properties parameter.
     * Always creates a new instance of the consumer because
     * {@link KafkaConsumer} is not thread-safe.
     * The caller is responsible for closing the consumer instance.
     *
     * @param properties mapping properties to merge with data connection options.
     * @return consumer instance
     */
    @Nonnull
    public <K, V> Consumer<K, V> newConsumer(Properties properties) {
        if (getConfig().isShared()) {
            throw new IllegalArgumentException("KafkaConsumer is not thread-safe and can't be used "
                    + "with shared DataConnection '" + getConfig().getName() + "'");
        }
        return new KafkaConsumer<>(Util.mergeProps(getConfig().getProperties(), properties));
    }

    /**
     * Returns an instance of {@link KafkaProducer} based on the DataConnection
     * configuration.
     * <p>
     * The caller is responsible for closing the producer instance.
     * For non-shared producers the producer will be closed immediately
     * upon calling close.
     * For shared producers the producer will be closed when all users
     * close the returned instance and this DataConnection is released by
     * calling {@link #release()}
     * <p>
     *
     * @param transactionalId transaction id to pass as 'transactional.id'
     *                        property to a new KafkaProducer instance,
     *                        must be null for shared producer
     */
    @Nonnull
    public <K, V> KafkaProducer<K, V> getProducer(@Nullable String transactionalId) {
        if (getConfig().isShared()) {
            if (transactionalId != null) {
                throw new IllegalArgumentException("Cannot use transactions with shared "
                        + "KafkaProducer for DataConnection" + getConfig().getName());
            }
            retain();
            //noinspection unchecked
            return (KafkaProducer<K, V>) producerSupplier.get();
        } else {
            if (transactionalId != null) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                Map<String, Object> castProperties = (Map) getConfig().getProperties();
                Map<String, Object> copy = new HashMap<>(castProperties);
                copy.put("transactional.id", transactionalId);
                return new KafkaProducer<>(copy);
            } else {
                return new KafkaProducer<>(getConfig().getProperties());
            }
        }
    }

    /**
     * Returns an instance of {@link KafkaProducer} based on the DataConnection
     * configuration and given properties.
     * <p>
     * The caller is responsible for closing the producer instance.
     * For non-shared producers the producer will be closed immediately
     * upon calling close.
     * For shared producers the producer will be closed when all users
     * close the returned instance and this DataConnection is released by
     * calling {@link #release()}
     * <p>
     *
     * @param transactionalId transaction id to pass as 'transactional.id'
     *                        property to a new KafkaProducer instance,
     *                        must be null for shared producer
     * @param properties      properties. E.g, SQL mappings provide separate
     *                        options, and they should be merged with
     *                        data connection's properties. These properties
     *                        have higher priority than data connection properties.
     *                        If {@link KafkaDataConnection} is shared, then
     *                        {@link HazelcastException} would be thrown.
     */
    @Nonnull
    public <K, V> KafkaProducer<K, V> getProducer(
            @Nullable String transactionalId,
            @Nonnull Properties properties) {
        Properties configProperties = getConfig().getProperties();

        boolean inputPropsAreSubsetOfConfigProps = inputPropsAreSubsetOfConfigProps(properties);
        if (getConfig().isShared()) {
            if (!properties.isEmpty() && !inputPropsAreSubsetOfConfigProps) {
                throw new HazelcastException("For shared Kafka producer, please provide all serialization options" +
                        "at the DATA CONNECTION level (i.e. 'key.serializer')." +
                        " Only 'keyFormat' and 'valueFormat' are required at the mapping level," +
                        " however these options are ignored currently.");
            } else {
                retain();
                //noinspection unchecked
                return (KafkaProducer<K, V>) producerSupplier.get();
            }
        }

        // Next, we have only non-shared producer creation process.
        Properties props = Util.mergeProps(configProperties, properties);

        if (transactionalId != null) {
            props.put("transactional.id", transactionalId);
        }
        return new KafkaProducer<>(props);
    }

    @Override
    public synchronized void destroy() {
        if ((producerSupplier != null) && (producerSupplier.remembered() != null)) {
            producerSupplier.remembered().doClose();
            producerSupplier = null;
        }
    }

    private boolean inputPropsAreSubsetOfConfigProps(Properties inputProps) {
        Properties configProperties = getConfig().getProperties();
        for (Object key : inputProps.keySet()) {
            if (!configProperties.containsKey(key) || !configProperties.get(key).equals(inputProps.get(key))) {
                return false;
            }
        }
        return true;
    }
}
