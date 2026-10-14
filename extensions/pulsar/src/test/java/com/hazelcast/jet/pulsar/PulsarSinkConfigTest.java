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
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
@ParallelTest
class PulsarSinkConfigTest {

    @Test
    void clonesAll() {
        PulsarSinkConfig<String, String> sinkConfig = buildObject();
        PulsarSinkConfig<String, String> clone = new PulsarSinkConfig<>(sinkConfig);
        assertThat(clone).usingRecursiveComparison().isEqualTo(sinkConfig);
    }

    @Test
    void serializesAll() throws IOException {
        PulsarSinkConfig<String, String> sourceConfig = buildObject();
        var serializationService = createSerializationService();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectDataOutputStream out = new ObjectDataOutputStream(baos, serializationService)) {

            serializationService.writeObject(out, sourceConfig);

            byte[] serialized = baos.toByteArray();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                 ObjectDataInputStream in = new ObjectDataInputStream(bais, serializationService)) {
                PulsarSinkConfig<String, String> deserialized = serializationService.readObject(in);

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

    private PulsarSinkConfig<String, String> buildObject() {
        PulsarSinkConfig<String, String> sinkConf = new PulsarSinkConfig<>();
        sinkConf.setDataConnectionRef(DataConnectionRef.dataConnectionRef("testAbc"));
        sinkConf.setLocalParallelism(13);
        sinkConf.setOptions(Map.of("test", "123"));
        sinkConf.setTopic("topic1");
        sinkConf.setSchemaSupplier(() -> Schema.STRING);

        sinkConf.setTransactional(true);
        sinkConf.setExtractValueFn(Object::toString);
        sinkConf.setExtractKeyFn(s -> s);
        sinkConf.setExtractPropertiesFn(s -> Map.of("prop1", "value"));
        sinkConf.setExtractTimestampFn(s -> System.currentTimeMillis());
        sinkConf.setTransactionTimeout(Duration.ofHours(123));
        sinkConf.setProducerCustomizer(producer -> producer.producerName("someName"));
        sinkConf.validate();
        return sinkConf;
    }

}
