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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.jet.datamodel.TwoBags.twoBags;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class TwoBagsTest {
    private TwoBags<String, String> bags;

    @Test
    public void when_useEmptyFactory_then_emptyBags() {
        // When
        bags = twoBags();

        // Then
        assertTrue(bags.bag0().isEmpty());
        assertTrue(bags.bag1().isEmpty());
    }

    @Test
    public void when_useFactory_then_bagsHaveItems() {
        // Given
        List<String> bag0 = asList("a0", "b0");
        List<String> bag1 = asList("a1", "b1");

        // When
        bags = twoBags(bag0, bag1);

        // Then
        assertEquals(bag0, bags.bag0());
        assertEquals(bag1, bags.bag1());
    }

    @Test
    public void when_combineWith_then_hasAllItems() {
        // Given
        bags = twoBags(singletonList("100"), singletonList("200"));
        TwoBags<String, String> bags_b =
                twoBags(singletonList("101"), singletonList("201"));

        // When
        bags.combineWith(bags_b);

        // Then
        assertEquals(twoBags(
                asList("100", "101"),
                asList("200", "201")
        ), bags);
    }

    @Test
    public void when_equalBags_then_equalsTrueAndHashCodesEqual() {
        // Given
        bags = twoBags(singletonList("100"), singletonList("200"));
        TwoBags<String, String> bags_b = twoBags(singletonList("100"), singletonList("200"));

        // When - Then
        assertTrue(bags.equals(bags_b));
        assertEquals(bags.hashCode(), bags_b.hashCode());
    }

    @Test
    public void when_unequalBags_then_equalsFalse() {
        // Given
        bags = twoBags(singletonList("100"), singletonList("200"));
        TwoBags<String, String> bags_b = twoBags(singletonList("100"), singletonList("201"));

        // When - Then
        assertFalse(bags.equals(bags_b));
    }

    @Test
    public void when_toString_then_noFailures() {
        assertNotNull(twoBags(singletonList("100"), singletonList("200")).toString());
        assertNotNull(twoBags().toString());
    }
}
