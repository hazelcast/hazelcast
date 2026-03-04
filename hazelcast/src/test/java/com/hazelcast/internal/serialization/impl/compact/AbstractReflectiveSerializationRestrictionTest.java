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
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.Accessors;
import org.assertj.core.api.ThrowableAssert;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public abstract class AbstractReflectiveSerializationRestrictionTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameter
    public Object underTest;

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    protected abstract AbstractSerializationService getSerializationServiceToTest(HazelcastInstance hz);

    protected abstract boolean isSuccessExpected();

    @Test
    public void testMemberWrite() {
        HazelcastInstance hz = factory.newHazelcastInstance(getConfig());
        assertReflectiveCompactSerializationOutcome(() -> getSerializationServiceToTest(hz).toBytes(underTest));
    }

    @Test
    public void testMemberRead() {
        assumeThat(underTest.getClass().isArray()).as("Top-level array cannot be serialized").isFalse();

        HazelcastInstance hz = factory.newHazelcastInstance(getConfig());
        byte[] bytesToRead = writeRestrictedData(hz);

        assertReflectiveCompactSerializationOutcome(() -> getSerializationServiceToTest(hz).toObject(new HeapData(bytesToRead)));
    }

    @Test
    public void testClientWrite() {
        factory.newHazelcastInstance(getConfig());
        HazelcastInstance hz = factory.newHazelcastClient(getClientConfig());
        assertReflectiveCompactSerializationOutcome(() -> getSerializationServiceToTest(hz).toBytes(underTest));
    }

    @Test
    public void testClientRead() {
        assumeThat(underTest.getClass().isArray()).as("Top-level array cannot be serialized").isFalse();

        factory.newHazelcastInstance(getConfig());
        HazelcastClientProxy hz = (HazelcastClientProxy) factory.newHazelcastClient(getClientConfig());

        byte[] bytesToRead = writeRestrictedData(hz);

        assertReflectiveCompactSerializationOutcome(() -> getSerializationServiceToTest(hz).toObject(new HeapData(bytesToRead)));
    }

    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    protected final AbstractSerializationService getActualSerializationService(HazelcastInstance hz) {
        if (hz instanceof HazelcastClientProxy proxy) {
            return (AbstractSerializationService) proxy.getSerializationService();
        } else {
            return (AbstractSerializationService) Accessors.getSerializationService(hz);
        }
    }

    protected final AbstractSerializationService overrideBlockList(HazelcastInstance hz, ClassFilter override) {
        AbstractSerializationService ss = getActualSerializationService(hz);
        CompactSerializationConfig config = hz.getConfig().getSerializationConfig().getCompactSerializationConfig();
        SchemaService schemaService = hz instanceof HazelcastClientProxy clientProxy ? clientProxy.client.getSchemaService()
                : Accessors.getNode(hz).getSchemaService();

        AbstractSerializationService spiedSs = spy(ss);
        CompactStreamSerializerAdapter adjustedAdapter = new CompactStreamSerializerAdapter(
                new CompactStreamSerializer(ss, config, ss.getManagedContext(), schemaService, null, override, null));

        doReturn(adjustedAdapter).when(spiedSs).serializerForClass(any(), eq(false));
        doReturn(adjustedAdapter).when(spiedSs).serializerFor(SerializationConstants.TYPE_COMPACT);
        return spiedSs;
    }

    private byte[] writeRestrictedData(HazelcastInstance hz) {
        return overrideBlockList(hz, null).toBytes(underTest);
    }

    private void assertReflectiveCompactSerializationOutcome(ThrowableAssert.ThrowingCallable block) {
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
