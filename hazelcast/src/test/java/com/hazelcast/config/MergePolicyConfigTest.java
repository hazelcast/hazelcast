/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.internal.config.MergePolicyConfigReadOnly;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MergePolicyConfigTest {

    private MergePolicyConfig config = new MergePolicyConfig();

    @Test
    public void testConstructor_withParameters() {
        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 100);
        new MergePolicyConfig(PassThroughMergePolicy.class.getName(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withParameters_withNullPolicy() {
        new MergePolicyConfig(null, 100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withParameters_withEmptyPolicy() {
        new MergePolicyConfig("", 100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withParameters_withZeroBatchSize() {
        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withParameters_withNegativeBatchSize() {
        new MergePolicyConfig(DiscardMergePolicy.class.getName(), -1);
    }

    @Test
    public void setPolicy() {
        config.setPolicy(DiscardMergePolicy.class.getName());

        assertEquals(DiscardMergePolicy.class.getName(), config.getPolicy());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setPolicy_withNull() {
        config.setPolicy(null);
    }

    @Test
    public void setBatchSize() {
        config.setBatchSize(1234);

        assertEquals(1234, config.getBatchSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBatchSize_withZero() {
        config.setBatchSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBatchSize_withNegative() {
        config.setBatchSize(-1);
    }

    @Test
    public void testToString() {
        config.setPolicy(HigherHitsMergePolicy.class.getName());
        config.setBatchSize(2342);

        String configString = config.toString();
        assertThat(configString, containsString("MergePolicyConfig"));
        assertThat(configString, containsString("policy='" + HigherHitsMergePolicy.class.getName() + "'"));
        assertThat(configString, containsString("batchSize=2342"));
    }

    @Test
    public void getAsReadOnly() {
        config.setPolicy(HigherHitsMergePolicy.class.getName());
        config.setBatchSize(2342);

        MergePolicyConfig readOnly = new MergePolicyConfigReadOnly(config);

        assertEquals(config.getPolicy(), readOnly.getPolicy());
        assertEquals(config.getBatchSize(), readOnly.getBatchSize());
    }

    @Test
    public void testSerialization() {
        config.setPolicy(DiscardMergePolicy.class.getName());
        config.setBatchSize(1234);

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        MergePolicyConfig deserialized = serializationService.toObject(serialized);

        assertEquals(config.getPolicy(), deserialized.getPolicy());
        assertEquals(config.getBatchSize(), deserialized.getBatchSize());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(MergePolicyConfig.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .withPrefabValues(MergePolicyConfig.class,
                        new MergePolicyConfig(PutIfAbsentMergePolicy.class.getName(), 1000),
                        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 300))
                .verify();
    }
}
