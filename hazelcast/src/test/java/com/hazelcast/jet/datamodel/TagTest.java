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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
public class TagTest {
    private Tag tag;

    @Test
    public void when_factory_then_getWithSpecifiedIndex() {
        assertEquals(42, tag(42).index());
    }

    @Test
    public void when_tag012_then_indices012() {
        assertEquals(0, tag0().index());
        assertEquals(1, tag1().index());
        assertEquals(2, tag2().index());
    }

    @Test
    public void when_tagsEqual_then_equalsTrueAndHashCodesEqual() {
        // Given
        tag = tag(42);
        Tag tagB = tag(42);

        // When - Then
        assertTrue(tag.equals(tagB));
        assertTrue(tag.hashCode() == tagB.hashCode());
    }

    @Test
    public void when_tagsUnequal_then_equalsFalse() {
        // Given
        tag = tag(41);
        Tag tagB = tag(42);

        // When - Then
        assertFalse(tag.equals(tagB));
    }

    @Test
    public void compareTo_ordersByIndex() {
        assertTrue(tag(42).compareTo(tag(43)) < 0);
        assertTrue(tag(42).compareTo(tag(42)) == 0);
        assertTrue(tag(42).compareTo(tag(41)) > 0);
    }

    @Test
    public void when_toString_then_noFailures() {
        assertNotNull(tag0().toString());
        assertNotNull(tag(42).toString());
    }
}
