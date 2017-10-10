/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.BagsByTag.bagsByTag;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class BagsByTagTest {
    private BagsByTag bbt;

    @Before
    public void before() {
        bbt = new BagsByTag();
    }

    @Test
    public void when_useFactory_then_everythingInPlace() {
        // When
        List<Integer> bag0 = singletonList(100);
        List<Integer> bag1 = singletonList(101);
        bbt = bagsByTag(tag0(), bag0, tag1(), bag1);

        // Then
        assertEquals(bag0, bbt.bag(tag0()));
        assertEquals(bag1, bbt.bag(tag1()));
    }

    @Test
    public void when_ensureBag_then_bagThere() {
        // When
        bbt.ensureBag(tag0());

        // Then
        assertNotNull(bbt.bag(tag0()));
    }

    @Test
    public void when_ensureBag_then_getBagGetsTheSame() {
        // When
        Collection<Object> ensuredBag = bbt.ensureBag(tag0());
        Collection<Object> gotBag = bbt.bag(tag0());

        // Then
        assertSame(ensuredBag, gotBag);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_getNonExistentBag_then_exception() {
        bbt.bag(tag0());
    }

    @Test
    public void when_combineWith_then_hasAllItems() {
        // Given
        bbt = bagsByTag(tag1(), singletonList(100), tag2(), singletonList(200));
        BagsByTag bbt_b = bagsByTag(tag1(), singletonList(101), tag2(), singletonList(201));

        // When
        bbt.combineWith(bbt_b);

        // Then
        assertEquals(bagsByTag(
               tag1(), asList(100, 101),
               tag2(), asList(200, 201)
        ), bbt);
    }

    @Test
    public void when_twoEqualBags_then_equalsTrueAndHashCodesEqual() {
        // Given
        BagsByTag bbt_b = new BagsByTag();
        bbt.ensureBag(tag0()).add(100);
        bbt_b.ensureBag(tag0()).add(100);

        // When - Then
        assertTrue(bbt_b.equals(bbt));
        assertTrue(bbt_b.hashCode() == bbt.hashCode());
    }

    @Test
    public void when_twoUnequalBags_then_equalsFalse() {
        // Given
        BagsByTag bbt_b = new BagsByTag();
        bbt.ensureBag(tag0()).add(100);
        bbt_b.ensureBag(tag1()).add(100);

        // When - Then
        assertFalse(bbt_b.equals(bbt));
    }

    @Test
    public void when_entrySet_then_getComponentsEntrySet() {
        // Given
        bbt.ensureBag(tag0()).add(100);
        bbt.ensureBag(tag1()).add(101);

        // When
        Set<Entry<Tag<?>, Collection>> entrySet = bbt.entrySet();

        // Then
        Set<Entry<Tag<Object>, List<Integer>>> expected = new HashSet<>(asList(
                entry(tag0(), singletonList(100)),
                entry(tag1(), singletonList(101))));
        assertEquals(expected, entrySet);
    }

    @Test
    public void when_putBag_then_getIt() {
        // When
        ArrayList<Integer> bag0 = new ArrayList<>(singletonList(100));
        bbt.put(tag0(), bag0);

        // Then
        assertSame(bag0, bbt.bag(tag0()));
    }

    @Test
    public void when_toString_then_noFailure() {
        assertNotNull(bbt.toString());
    }
}
