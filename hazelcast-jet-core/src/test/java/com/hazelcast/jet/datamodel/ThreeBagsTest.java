/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.jet.datamodel.ThreeBags.threeBags;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ThreeBagsTest {
    private ThreeBags<String, String, String> bags;

    @Test
    public void when_useEmptyFactory_then_emptyBags() {
        // When
        bags = threeBags();

        // Then
        assertTrue(bags.bag0().isEmpty());
        assertTrue(bags.bag1().isEmpty());
        assertTrue(bags.bag2().isEmpty());
    }

    @Test
    public void when_useFactory_then_bagsHaveItems() {
        // Given
        List<String> bag0 = asList("a0", "b0");
        List<String> bag1 = asList("a1", "b1");
        List<String> bag2 = asList("a2", "b2");

        // When
        bags = threeBags(bag0, bag1, bag2);

        // Then
        assertEquals(bag0, bags.bag0());
        assertEquals(bag1, bags.bag1());
        assertEquals(bag2, bags.bag2());
    }

    @Test
    public void when_combineWith_then_hasAllItems() {
        // Given
        bags = threeBags(singletonList("100"), singletonList("200"), singletonList("300"));
        ThreeBags<String, String, String> bags_b =
                threeBags(singletonList("101"), singletonList("201"), singletonList("301"));

        // When
        bags.combineWith(bags_b);

        // Then
        assertEquals(threeBags(
                asList("100", "101"),
                asList("200", "201"),
                asList("300", "301")
        ), bags);
    }

    @Test
    public void when_equalBags_then_equalsTrueAndHashCodesEqual() {
        // Given
        bags = threeBags(singletonList("100"), singletonList("200"), singletonList("300"));
        ThreeBags<String, String, String> bags_b =
                threeBags(singletonList("100"), singletonList("200"), singletonList("300"));

        // When - Then
        assertTrue(bags.equals(bags_b));
        assertTrue(bags.hashCode() == bags_b.hashCode());
    }

    @Test
    public void when_unequalBags_then_equalsFalse() {
        // Given
        bags = threeBags(singletonList("100"), singletonList("200"), singletonList("300"));
        ThreeBags<String, String, String> bags_b =
                threeBags(singletonList("100"), singletonList("200"), singletonList("301"));

        // When - Then
        assertFalse(bags.equals(bags_b));
    }

    @Test
    public void when_toString_then_noFailures() {
        assertNotNull(threeBags(singletonList("100"), singletonList("200"), singletonList("300")).toString());
        assertNotNull(threeBags().toString());
    }
}
