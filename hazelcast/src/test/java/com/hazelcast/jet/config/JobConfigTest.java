/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class JobConfigTest extends JetTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_setName_thenReturnsName() {
        // When
        JobConfig config = new JobConfig();
        String name = "myJobName";
        config.setName(name);

        // Then
        assertEquals(name, config.getName());
    }

    @Test
    public void when_enableSplitBrainProtection_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setSplitBrainProtection(true);

        // Then
        assertTrue(config.isSplitBrainProtectionEnabled());
    }

    @Test
    public void when_enableAutoScaling_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setAutoScaling(true);

        // Then
        assertTrue(config.isAutoScaling());
    }

    @Test
    public void when_setProcessingGuarantee_thenReturnsProcessingGuarantee() {
        // When
        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);

        // Then
        assertEquals(EXACTLY_ONCE, config.getProcessingGuarantee());
    }

    @Test
    public void when_setSnapshotIntervalMillis_thenReturnsSnapshotIntervalMillis() {
        // When
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(50);

        // Then
        assertEquals(50, config.getSnapshotIntervalMillis());
    }

    @Test
    public void when_losslessRestartEnabled_then_openSourceMemberDoesNotStart() {
        // When
        Config config = new Config();
        config.getJetConfig().getInstanceConfig().setLosslessRestartEnabled(true);

        // Then
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("Lossless Restart is not available in the open-source version of Hazelcast Jet");
        createJetMember(config);
    }

    @Test
    public void when_registerSerializerTwice_then_fails() {
        // Given
        JobConfig config = new JobConfig();
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // When
        // Then
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Serializer for class java.lang.Object already registered");
        config.registerSerializer(Object.class, ObjectSerializer.class);
    }

    @Test
    public void when_registerSerializer() {
        // Given
        JobConfig config = new JobConfig();

        // When
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // Then
        Map<String, String> serializerConfigs = config.getSerializerConfigs();
        assertThat(serializerConfigs.entrySet(), hasSize(1));
        assertThat(serializerConfigs.keySet(), contains(Object.class.getName()));
        assertThat(serializerConfigs.values(), contains(ObjectSerializer.class.getName()));
    }

    @Test
    public void when_default_then_suspendOnFailureDisabled() {
        // Given
        JobConfig config = new JobConfig();

        // Then
        assertThat(config.isSuspendOnFailure(), equalTo(FALSE));
    }

    @Test
    public void when_suspendOnFailureSet_then_suspendOnFailureIsReturned() {
        // Given
        JobConfig config = new JobConfig();

        // When
        config.setSuspendOnFailure(true);

        // Then
        assertThat(config.isSuspendOnFailure(), equalTo(TRUE));
    }

    private static class ObjectSerializer implements StreamSerializer<Object> {

        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void write(ObjectDataOutput out, Object object) {
        }

        @Override
        public Object read(ObjectDataInput in) {
            return null;
        }
    }
}
