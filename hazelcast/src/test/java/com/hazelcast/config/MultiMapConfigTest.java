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

import com.hazelcast.config.MultiMapConfig.ValueCollectionType;
import com.hazelcast.internal.config.MultiMapConfigReadOnly;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Locale;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertSame;

/**
 * Tests for {@link MultiMapConfig} class.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapConfigTest {

    private MultiMapConfig multiMapConfig = new MultiMapConfig();

    @Test
    public void testValueCollectionTypeSelection() {
        Locale locale = Locale.getDefault();
        multiMapConfig.setValueCollectionType("list");
        try {
            assertSame(ValueCollectionType.LIST, multiMapConfig.getValueCollectionType());
            Locale.setDefault(new Locale("tr"));
            assertSame(ValueCollectionType.LIST, multiMapConfig.getValueCollectionType());
        } finally {
            Locale.setDefault(locale);
        }
    }

    @Test
    public void testMergePolicy() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName())
                .setBatchSize(2342);

        multiMapConfig.setMergePolicyConfig(mergePolicyConfig);

        assertSame(mergePolicyConfig, multiMapConfig.getMergePolicyConfig());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(MultiMapConfig.class)
                .suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS)
                .withPrefabValues(MultiMapConfigReadOnly.class,
                        new MultiMapConfigReadOnly(new MultiMapConfig("red")),
                        new MultiMapConfigReadOnly(new MultiMapConfig("black")))
                .withPrefabValues(MergePolicyConfig.class,
                        new MergePolicyConfig(PutIfAbsentMergePolicy.class.getName(), 100),
                        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 200))
                .verify();
    }
}
