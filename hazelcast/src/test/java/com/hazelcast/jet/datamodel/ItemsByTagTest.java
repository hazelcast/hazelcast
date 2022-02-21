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

package com.hazelcast.jet.datamodel;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ItemsByTagTest {
    private ItemsByTag ibt;

    @Before
    public void before() {
        ibt = new ItemsByTag();
    }

    @Test
    public void when_useFactory_then_everythingInPlace() {
        // When
        ibt = itemsByTag(tag0(), "a", tag1(), "b", tag2(), "c", tag(3), "d");

        // Then
        assertEquals("a", ibt.get(tag0()));
        assertEquals("b", ibt.get(tag1()));
        assertEquals("c", ibt.get(tag2()));
        assertEquals("d", ibt.get(tag(3)));
    }

    @Test
    public void when_put_then_getIt() {
        // When
        ibt.put(tag2(), "x");

        // Then
        assertEquals("x", ibt.get(tag2()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_getNonexistent_then_exception() {
        ibt.get(tag1());
    }

    @Test
    public void when_equal_then_equalsTrueAndHashCodesEqual() {
        // Given
        ibt = itemsByTag(tag0(), "a", tag1(), "b", tag2(), "c", tag(3), "d");
        ItemsByTag ibt2 = itemsByTag(tag0(), "a", tag1(), "b", tag2(), "c", tag(3), "d");

        // When - Then
        assertTrue(ibt.equals(ibt2));
        assertTrue(ibt.hashCode() == ibt2.hashCode());
    }

    @Test
    public void when_nonequal_then_equalsFalse() {
        // Given
        ibt = itemsByTag(tag0(), "a");
        ItemsByTag ibt2 = itemsByTag(tag1(), "a");

        // When - Then
        assertFalse(ibt.equals(ibt2));
    }

    @Test
    public void when_toString_then_noFailures() {
        assertNotNull(ibt.toString());
        ibt.put(tag2(), "x");
        assertNotNull(ibt.toString());
    }

    @Test
    public void when_entrySet_then_hasAllItems() {
        // Given
        ibt.put(tag2(), "x");

        // When
        Set<Entry<Tag<?>, Object>> entrySet = ibt.entrySet();

        // Then
        assertNotNull(entrySet);
        Iterator<Entry<Tag<?>, Object>> it = entrySet.iterator();
        assertTrue(it.hasNext());
        assertEquals(entry(tag2(), "x"), it.next());
    }
}
