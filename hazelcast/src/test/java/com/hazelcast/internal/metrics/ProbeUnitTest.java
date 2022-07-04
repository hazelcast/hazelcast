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

package com.hazelcast.internal.metrics;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProbeUnitTest {

    // set to the current unit count
    // add "RU_COMPAT_X_Y" when introducing a new unit
    private static final int HIGHEST_UNIT_ORDINAL_IN_LAST_VERSION = 7;
    private static final int UNIT_COUNT_IN_CURRENT_VERSION = 8;

    @Test
    public void testNoReorder() {
        assertEquals(0, ProbeUnit.BYTES.ordinal());
        assertEquals(1, ProbeUnit.MS.ordinal());
        assertEquals(2, ProbeUnit.NS.ordinal());
        assertEquals(3, ProbeUnit.PERCENT.ordinal());
        assertEquals(4, ProbeUnit.COUNT.ordinal());
        assertEquals(5, ProbeUnit.BOOLEAN.ordinal());
        assertEquals(6, ProbeUnit.ENUM.ordinal());
        assertEquals(7, ProbeUnit.US.ordinal());
    }

    @Test
    public void testUnitCount() {
        // this test intentionally compares against a constant value
        // the purpose of this test is to fail if a new unit is introduced to make sure the other test cases are
        // getting updated with the new unit
        // please add the unit with its ordinal to testNoReorder and increase the expected count here
        assertEquals(UNIT_COUNT_IN_CURRENT_VERSION, ProbeUnit.values().length);
    }

    @Test
    public void testNewUnitIsMarkedAsNew() {
        ProbeUnit[] units = ProbeUnit.values();
        for (int i = HIGHEST_UNIT_ORDINAL_IN_LAST_VERSION + 1; i < units.length; i++) {
            assertTrue(units[i].isNewUnit());
        }
    }
}
