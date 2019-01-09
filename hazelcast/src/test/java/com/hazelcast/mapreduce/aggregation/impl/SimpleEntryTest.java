/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SimpleEntryTest {

    private SimpleEntry<String, String> simpleEntry = new SimpleEntry<String, String>();

    @Test
    public void testGetKey() {
        assertNull(simpleEntry.getKey());
    }

    @Test
    public void testGetValue() {
        assertNull(simpleEntry.getValue());
    }

    @Test
    public void testGetTargetObject() {
        simpleEntry.setKey("targetKey");
        simpleEntry.setValue("targetValue");

        assertEquals("targetKey", simpleEntry.getTargetObject(true));
        assertEquals("targetValue", simpleEntry.getTargetObject(false));
    }

    @Test
    public void testSetKey() {
        simpleEntry.setKey("key");

        assertEquals("key", simpleEntry.getKey());
    }

    @Test
    public void testSetValue() {
        simpleEntry.setValue("value");

        assertEquals("value", simpleEntry.getValue());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetKeyData() {
        simpleEntry.getKeyData();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetValueData() {
        simpleEntry.getValueData();
    }
}
