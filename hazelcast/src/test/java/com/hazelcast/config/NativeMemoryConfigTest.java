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

import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeMemoryConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(NativeMemoryConfig.class)
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .verify();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSizeCompatibility() {
        Capacity cap = new Capacity(128, MemoryUnit.BYTES);
        MemorySize memSize = new MemorySize(128, MemoryUnit.BYTES);

        NativeMemoryConfig conf = new NativeMemoryConfig();
        conf.setSize(cap);
        conf.setSize(memSize);
        Capacity capacityReturned = conf.getSize();
        MemorySize memorySizeReturned = conf.getSize();

        assertThat(capacityReturned).isEqualToComparingFieldByField(cap);
        assertThat(memorySizeReturned).isEqualToComparingFieldByField(memSize);
    }
}
