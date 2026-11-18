/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.jet.pulsar.impl.WritePulsarP;
import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SerialTest
class WritePulsarPDeduplicationGateTest extends PulsarTestSupport {
    private HazelcastInstance instance;
    private WritePulsarP<String, String> sink;
    private PulsarClient client;

    @AfterEach
    void cleanup() throws Exception {
        if (sink != null) {
            sink.close();
        }
        if (client != null) {
            client.close();
        }
        if (instance != null) {
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    void throwsWhenExactlyOnceRequestedButDeduplicationDisabled() throws Exception {
        String topic = createTopicWithDeduplication(false);
        init(topic, EXACTLY_ONCE, true);

        assertThatThrownBy(this::initSink)
                .isInstanceOf(JetException.class)
                .hasMessageContaining("Deduplication is not enabled on the broker");
    }

    @Test
    void doesNotCheckDeduplicationWhenAtLeastOnce() throws Exception {
        String topic = createTopicWithDeduplication(false);
        init(topic, AT_LEAST_ONCE, true);

        // AT_LEAST_ONCE short-circuits checkDeduplicationStatus() before any admin call -
        // succeeds even though the topic has deduplication disabled
        assertThatCode(this::initSink).doesNotThrowAnyException();
    }

    @Test
    void doesNotThrowWhenExactlyOnceRequestedWithoutDataConnectionOrHttpServiceUrl() throws Exception {
        String topic = createTopicWithDeduplication(false);
        // Neither a DataConnectionRef nor an explicit httpServiceUrl is configured - the gate
        // cannot check broker-side deduplication status at all in this case. Today it only logs
        // severe() and proceeds, rather than failing fast the way the other two cases do.
        init(topic, EXACTLY_ONCE, false);

        assertThatCode(this::initSink).doesNotThrowAnyException();
    }

    @Test
    void succeedsWhenExactlyOnceRequestedAndDeduplicationEnabled() throws Exception {
        String topic = createTopicWithDeduplication(true);
        init(topic, EXACTLY_ONCE, true);

        assertThatCode(this::initSink).doesNotThrowAnyException();
    }

    private String createTopicWithDeduplication(boolean enabled) throws Exception {
        String topic = "persistent://public/default/" + randomName();
        try (var admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(topic);
            admin.topicPolicies().setDeduplicationStatus(topic, enabled);
        }
        return topic;
    }

    private void init(String topic, ProcessingGuarantee guarantee, boolean withDataConnection) throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).enableTransaction(true).build();

        var config = new PulsarSinkConfig<String, String>();
        config.setTopic(topic);
        config.setConnectionSupplier(() -> client);
        config.setSchemaSupplier(() -> Schema.STRING);
        config.setExtractValueFn(value -> value);
        config.setOptions(Map.of());
        config.setTransactional(true);
        config.setTransactionTimeout(Duration.ofMinutes(1));
        config.setProducerCustomizer(builder -> builder.enableBatching(false).sendTimeout(30, SECONDS));

        Config conf = smallInstanceConfig();
        if (withDataConnection) {
            conf.addDataConnectionConfig(pulsarDataConnectionConf("pulsar",
                    pulsarContainer.getPulsarBrokerUrl(), pulsarContainer.getHttpServiceUrl(), true));
            config.setDataConnectionRef(DataConnectionRef.dataConnectionRef("pulsar"));
        }
        instance = createHazelcastInstance(conf);

        sink = new WritePulsarP<>(config, client);
        contextFor(guarantee);
    }

    private TestProcessorContext context;

    private void contextFor(ProcessingGuarantee guarantee) {
        context = new TestProcessorContext()
                .setProcessingGuarantee(guarantee)
                .setHazelcastInstance(instance);
    }

    private void initSink() throws Exception {
        sink.init(new TestOutbox(new int[0], 1), context);
    }
}
