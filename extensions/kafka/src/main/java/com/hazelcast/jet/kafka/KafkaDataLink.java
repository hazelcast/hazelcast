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

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.DataLinkBase;
import com.hazelcast.datalink.DataLinkResource;
import com.hazelcast.jet.kafka.impl.NonClosingKafkaProducer;
import com.hazelcast.spi.annotation.Beta;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * {@link DataLink} implementation for Kafka.
 * <p>
 * KafkaDataLink is usable both for sources and sinks.
 * <p>
 * Sources obtain {@link KafkaConsumer} instances using {@link #newConsumer()}.
 * Because the KafkaConsumer is not thread-safe so the DataLink is used only
 * to keep the connection metadata. A new instance is returned each time this
 * method is called.
 * <p>
 * Sinks obtain {@link KafkaProducer} instances using {@link #getProducer(String)}.
 * The producer instance may be either shared or single-use depending on the
 * {@link DataLinkConfig#isShared()} setting. Shared producer instance is limited
 * to use with either no processing guarantees or at-least-once processing guarantees.
 * Use with exactly-once is not possible.
 * <p>
 * The properties must be configured for both consumers and producers.
 *
 * @since 5.3
 */
@Beta
public class KafkaDataLink extends DataLinkBase {

    private volatile NonClosingKafkaProducer<?, ?> producer;

    /**
     * Create {@link KafkaDataLink} based on given config
     */
    public KafkaDataLink(@Nonnull DataLinkConfig config) {
        super(config);

        if (config.isShared()) {
            producer = new NonClosingKafkaProducer<>(config.getProperties(), this::release);
        }
    }

    @Nonnull
    @Override
    public Collection<DataLinkResource> listResources() {
        try (AdminClient client = AdminClient.create(getConfig().getProperties())) {
            return client.listTopics().names().get()
                         .stream()
                         .sorted()
                         .map(n -> new DataLinkResource("topic", n))
                         .collect(Collectors.toList());
        } catch (ExecutionException | InterruptedException e) {
            throw new HazelcastException("Could not get list of topics for DataLink " + getConfig().getName(), e);
        }
    }

    /**
     * Creates new instance of {@link KafkaConsumer} based on the DataLink
     * configuration.
     * Always creates a new instance of the consumer because
     * {@link KafkaConsumer} is not thread-safe.
     * The caller is responsible for closing the consumer instance.
     *
     * @return consumer instance
     */
    @Nonnull
    public <K, V> Consumer<K, V> newConsumer() {
        return new KafkaConsumer<>(getConfig().getProperties());
    }

    /**
     * Returns an instance of {@link KafkaProducer} based on the DataLink
     * configuration.
     * <p>
     * The caller is responsible for closing the producer instance.
     * For non-shared producers the producer will be closed immediately
     * upon calling close.
     * For shared producers the producer will be closed when all users
     * close the returned instance and this DataLink is released by
     * calling {@link #release()}
     * <p>
     * @param transactionalId transaction id to pass as 'transactional.id'
     *                        property to a new KafkaProducer instance,
     *                        must be null for shared producer
     */
    @Nonnull
    public <K, V> KafkaProducer<K, V> getProducer(@Nullable String transactionalId) {
        if (getConfig().isShared()) {
            if (transactionalId != null) {
                throw new IllegalArgumentException("Cannot use transactions with shared "
                        + "KafkaProducer for DataLink" + getConfig().getName());
            }
            retain();
            //noinspection unchecked
            return (KafkaProducer<K, V>) producer;
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

    @Override
    public void destroy() {
        if (producer != null) {
            producer.doClose();
            producer = null;
        }
    }
}
