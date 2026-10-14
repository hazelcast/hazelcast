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

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.internal.serialization.impl.compact.InMemorySchemaService;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pulsar.impl.PulsarSourceConfig;
import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
@ParallelTest
class PulsarSourceConfigTest {

    @Test
    void clonesAll() {
        PulsarSourceConfig<String, String> sourceConfig = buildObject();
        PulsarSourceConfig<String, String> clone = new PulsarSourceConfig<>(sourceConfig);
        assertThat(clone).usingRecursiveComparison().isEqualTo(sourceConfig);
    }

    @Test
    void serializesAll() throws IOException {
        PulsarSourceConfig<String, String> sourceConfig = buildObject();
        var serializationService = createSerializationService();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectDataOutputStream out = new ObjectDataOutputStream(baos, serializationService)) {

            serializationService.writeObject(out, sourceConfig);

            byte[] serialized = baos.toByteArray();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                 ObjectDataInputStream in = new ObjectDataInputStream(bais, serializationService)) {
                PulsarSourceConfig<String, String> deserialized = serializationService.readObject(in);

                assertThat(deserialized).usingRecursiveComparison().isEqualTo(sourceConfig);
            }
        }
    }

    public static InternalSerializationService createSerializationService() {
        SchemaService schemaService = new InMemorySchemaService();
        return new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig())
                .build();
    }

    private PulsarSourceConfig<String, String> buildObject() {
        PulsarSourceConfig<String, String> sourceConfig = new PulsarSourceConfig<>();
        sourceConfig.setSubscriberName("test");
        sourceConfig.setProjectionFn(Message::getValue);
        sourceConfig.setBatchReceivePolicy(BatchReceivePolicy.DEFAULT_MULTI_TOPICS_DISABLE_POLICY);
        sourceConfig.setDataConnectionRef(DataConnectionRef.dataConnectionRef("testAbc"));
        sourceConfig.setLocalParallelism(13);
        sourceConfig.setConsumerCustomizer(cc -> cc.deadLetterPolicy(DeadLetterPolicy.builder().build()));
        sourceConfig.setOptions(Map.of("test", "123"));
        sourceConfig.setTopics(List.of("topic1", "topic2", "topic3"));
        sourceConfig.setSchemaSupplier(() -> Schema.STRING);
        sourceConfig.validate();
        return sourceConfig;
    }

}
