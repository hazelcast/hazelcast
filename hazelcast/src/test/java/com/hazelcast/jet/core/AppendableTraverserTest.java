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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AppendableTraverserTest {

    private AppendableTraverser t = new AppendableTraverser(2);

    @Test
    public void smokeTest() {
        assertTrue(t.isEmpty());
        t.append("1");
        assertFalse(t.isEmpty());
        assertEquals("1", t.next());
        assertTrue(t.isEmpty());
        assertNull(t.next());

        t.append("2");
        t.append("3");
        assertEquals("2", t.next());
        assertEquals("3", t.next());
        assertNull(t.next());
    }

    @Test
    public void test_flatMapperUsage() {
        // an instance of AppendableTraverser is repeatedly returned
        // from a flatMap function
        Traverser tt = Traversers.traverseItems(10, 20)
                                 .flatMap(item -> {
                    assertTrue(t.isEmpty());
                    t.append(item);
                    t.append(item + 1);
                    return t;
                });

        assertEquals(10, tt.next());
        assertEquals(11, tt.next());
        assertEquals(20, tt.next());
        assertEquals(21, tt.next());
        assertNull(tt.next());
    }
}
