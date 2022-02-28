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

package com.hazelcast.jet;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.idFromString;
import static com.hazelcast.jet.Util.idToString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UtilTest extends JetTestSupport {

    @Test
    public void when_entry() {
        Entry<String, Integer> e = entry("key", 1);
        assertEquals("key", e.getKey());
        assertEquals(Integer.valueOf(1), e.getValue());
    }

    @Test
    public void when_idToString() {
        assertEquals("0000-0000-0000-0000", idToString(0));
        assertEquals("0000-0000-0000-0001", idToString(1));
        assertEquals("7fff-ffff-ffff-ffff", idToString(Long.MAX_VALUE));
        assertEquals("8000-0000-0000-0000", idToString(Long.MIN_VALUE));
        assertEquals("ffff-ffff-ffff-ffff", idToString(-1));
        assertEquals("1122-10f4-7de9-8115", idToString(1234567890123456789L));
        assertEquals("eedd-ef0b-8216-7eeb", idToString(-1234567890123456789L));
    }

    @Test
    public void when_idFromString() {
        assertEquals(0, idFromString("0000-0000-0000-0000"));
        assertEquals(1, idFromString("0000-0000-0000-0001"));
        assertEquals(Long.MAX_VALUE, idFromString("7fff-ffff-ffff-ffff"));
        assertEquals(Long.MIN_VALUE, idFromString("8000-0000-0000-0000"));
        assertEquals(-1, idFromString("ffff-ffff-ffff-ffff"));
        assertEquals(1234567890123456789L, idFromString("1122-10f4-7de9-8115"));
        assertEquals(-1234567890123456789L, idFromString("eedd-ef0b-8216-7eeb"));
    }
}
