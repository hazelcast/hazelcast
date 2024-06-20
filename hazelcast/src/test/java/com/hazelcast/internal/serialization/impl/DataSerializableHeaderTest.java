/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataSerializableHeaderTest {

    @Test
    public void identified() {
        byte header = DataSerializableHeader.createHeader(true, false);

        assertTrue(DataSerializableHeader.isIdentifiedDataSerializable(header));
        assertFalse(DataSerializableHeader.isVersioned(header));
    }

    @Test
    public void versioned() {
        byte header = DataSerializableHeader.createHeader(false, true);

        assertFalse(DataSerializableHeader.isIdentifiedDataSerializable(header));
        assertTrue(DataSerializableHeader.isVersioned(header));
    }

    @Test
    public void all() {
        byte header = DataSerializableHeader.createHeader(true, true);

        assertTrue(DataSerializableHeader.isIdentifiedDataSerializable(header));
        assertTrue(DataSerializableHeader.isVersioned(header));
    }
}
