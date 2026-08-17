/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ExtraReflectiveCompactSerializationRestrictions;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.Accessors;
import org.assertj.core.api.ThrowableAssert;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

public abstract class AbstractReflectiveSerializationRestrictionTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory().withSerializationRestrictionsDisabled();

    @Parameter
    public Object underTest;

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    protected abstract boolean isSuccessExpected();

    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    @Test
    public void testMemberWrite() {
        HazelcastInstance hz = factory.newHazelcastInstance(getConfig());
        assertReflectiveCompactSerializationWriteOutcome(() -> getActualSerializationService(hz).toBytes(underTest));
    }

    @Test
    public void testMemberRead() {
        assumeThat(underTest.getClass().isArray()).as("Top-level array cannot be serialized").isFalse();

        HazelcastInstance hz = factory.newHazelcastInstance(getConfig());
        byte[] bytesToRead = writeRestrictedData(hz);

        assertReflectiveCompactSerializationReadOutcome(() -> getActualSerializationService(hz).toObject(new HeapData(bytesToRead)));
    }

    @Test
    public void testClientWrite() {
        factory.newHazelcastInstance(getConfig());
        HazelcastInstance hz = factory.newHazelcastClient(getClientConfig());
        assertReflectiveCompactSerializationWriteOutcome(() -> getActualSerializationService(hz).toBytes(underTest));
    }

    @Test
    public void testClientRead() {
        assumeThat(underTest.getClass().isArray()).as("Top-level array cannot be serialized").isFalse();

        factory.newHazelcastInstance(getConfig());
        HazelcastClientProxy hz = (HazelcastClientProxy) factory.newHazelcastClient(getClientConfig());

        byte[] bytesToRead = writeRestrictedData(hz);

        assertReflectiveCompactSerializationReadOutcome(() -> getActualSerializationService(hz).toObject(new HeapData(bytesToRead)));
    }

    private InternalSerializationService getActualSerializationService(HazelcastInstance hz) {
        if (hz instanceof HazelcastClientProxy proxy) {
            return proxy.getSerializationService();
        } else {
            return Accessors.getSerializationService(hz);
        }
    }

    private byte[] writeRestrictedData(HazelcastInstance hz) {
        InternalSerializationService ss = getActualSerializationService(hz);
        CompactSerializationConfig config = new CompactSerializationConfig(
                hz.getConfig().getSerializationConfig().getCompactSerializationConfig());
        config.setZeroConfigFilter(new JavaSerializationFilterConfig());

        SchemaService schemaService = hz instanceof HazelcastClientProxy clientProxy ? clientProxy.client.getSchemaService() : Accessors.getNode(
                hz).getSchemaService();

        CompactStreamSerializerAdapter adjustedAdapter = new CompactStreamSerializerAdapter(
                new CompactStreamSerializer(cls -> CompactStreamSerializerAdapter.class, config, ss.getManagedContext(),
                        schemaService, null, ExtraReflectiveCompactSerializationRestrictions.NO_RESTRICTIONS));

        try (BufferObjectDataOutput output = ss.createObjectDataOutput()) {
            // Partition
            output.writeInt(0, ByteOrder.BIG_ENDIAN);
            // Serializer type
            output.writeInt(SerializationConstants.TYPE_COMPACT, ByteOrder.BIG_ENDIAN);
            // Data
            adjustedAdapter.write(output, underTest);
            return output.toByteArray();
        } catch (IOException e) {
            throw new AssertionError("Unexpected error creating output", e);
        }
    }

    private void assertReflectiveCompactSerializationReadOutcome(Callable<Object> block) {
        Object result;
        try {
            result = block.call();
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception thrown on read", e);
        }
        if (isSuccessExpected()) {
            assertThat(result).isInstanceOf(underTest.getClass());
        } else {
            assertThat(result).isInstanceOf(DeserializedGenericRecord.class);
            assertThat(((DeserializedGenericRecord) result).getSchema().getTypeName()).isEqualTo(underTest.getClass().getName());
        }
    }

    private void assertReflectiveCompactSerializationWriteOutcome(ThrowableAssert.ThrowingCallable block) {
        if (isSuccessExpected()) {
            assertThatNoException().isThrownBy(block);
        } else {
            assertFailsReflectiveCompactSerialization(block);
        }
    }

    private void assertFailsReflectiveCompactSerialization(ThrowableAssert.ThrowingCallable block) {
        assertThatThrownBy(block).satisfiesAnyOf(
                e -> assertThat(e).isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class),
                e -> assertThat(e).hasRootCauseInstanceOf(ReflectiveCompactSerializationUnsupportedException.class),
                e -> {
                    assertThat(underTest.getClass().isArray()).as("Condition applies only to arrays").isTrue();
                    assertThat(e).rootCause()
                            .as("Top level array should not be compact-serializable")
                            .isInstanceOf(HazelcastSerializationException.class)
                            .hasMessageContaining("this type is not supported yet");
                });
    }
}
