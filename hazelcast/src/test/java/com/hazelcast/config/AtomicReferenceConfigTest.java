/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AtomicReferenceConfig.AtomicReferenceConfigReadOnly;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicReferenceConfigTest extends AbstractBasicConfigTest<AtomicReferenceConfig> {

    @Override
    protected AtomicReferenceConfig createConfig() {
        return new AtomicReferenceConfig();
    }

    @Test
    public void testConstructor_withName() {
        config = new AtomicReferenceConfig("myAtomicReference");

        assertEquals("myAtomicReference", config.getName());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(AtomicReferenceConfig.class)
                .allFieldsShouldBeUsedExcept("readOnly")
                .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
                .withPrefabValues(AtomicReferenceConfigReadOnly.class,
                        new AtomicReferenceConfigReadOnly(new AtomicReferenceConfig("red")),
                        new AtomicReferenceConfigReadOnly(new AtomicReferenceConfig("black")))
                .withPrefabValues(MergePolicyConfig.class,
                        new MergePolicyConfig(PutIfAbsentMergePolicy.class.getName(), 100),
                        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 200))
                .verify();
    }
}
