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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.internal.util.collection.WeightedEvictableList.WeightedItem;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category({SlowTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class WeightedEvictableListTest {

    @Test
    public void testNewItemStartsWithOneVote() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.addOrVote("a");
        assertEquals(1, list.getList().get(0).weight);
    }

    @Test
    public void testVoteFor() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        WeightedItem<String> weightedItem = list.addOrVote("a");
        list.voteFor(weightedItem);
        assertWeightsInOrder(list, 2);
    }

    @Test
    public void testAddDoesNotDuplicate() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.addOrVote("a");
        list.addOrVote("a");
        assertItemsInOrder(list, "a");
    }

    @Test
    public void testDuplicateAddIncreasesWeight() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.addOrVote("a");
        list.addOrVote("a");
        list.addOrVote("a");
        assertItemsInOrder(list, "a");
        assertWeightsInOrder(list, 3);
    }

    @Test
    public void testListReorganizesAfterEnoughVotes() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.addOrVote("c");
        list.addOrVote("b");
        list.addOrVote("b");
        list.addOrVote("a");
        list.addOrVote("a");
        list.addOrVote("a");
        assertItemsInOrder(list, "a", "b", "c");
        assertWeightsInOrder(list, 3, 2, 1);
    }

    @Test
    public void testListReorganizesAfterEnoughVotes_viaWeightedItem() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        WeightedItem<String> weightedC = list.addOrVote("c");
        WeightedItem<String> weightedB = list.addOrVote("b");
        WeightedItem<String> weightedA = list.addOrVote("a");

        list.voteFor(weightedB);
        list.voteFor(weightedA);
        list.voteFor(weightedA);
        assertItemsInOrder(list, "a", "b", "c");
        assertWeightsInOrder(list, 3, 2, 1);
    }

    @Test
    public void testListReorganizesAfterMaxSize() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 100);
        list.addOrVote("c");
        list.addOrVote("b");
        list.addOrVote("b");
        list.addOrVote("a");
        list.addOrVote("a");
        list.addOrVote("a");
        list.addOrVote("d");
        assertItemsInOrder(list, "a", "d");
        // weights are reset after max-size is reached and list is re-organized
        // new item is retained and it gets a vote
        assertWeightsInOrder(list, 0, 1);
    }

    @Test
    public void testScenario() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.addOrVote("a");
        list.addOrVote("b");
        list.addOrVote("c");

        // 3 iterations are here, now list will organize itself. Since all items have 1 vote, order does not change
        assertItemsInOrder(list, "a", "b", "c");
        assertWeightsInOrder(list, 1, 1, 1);

        list.addOrVote("c");
        list.addOrVote("b");
        list.addOrVote("c");
        // 3 iterations are here, now list will organize itself
        assertItemsInOrder(list, "c", "b", "a");
        assertWeightsInOrder(list, 3, 2, 1);

        list.addOrVote("b");
        list.addOrVote("c");
        list.addOrVote("d");
        // "c" has the most votes. 2 items has to go.

        assertItemsInOrder(list, "c", "d");
        assertWeightsInOrder(list, 0, 1);

        list.addOrVote("d"); //let the list re-organize now
        assertItemsInOrder(list, "d", "c");

        list.addOrVote("x");
        assertItemsInOrder(list, "d", "c", "x");
        assertWeightsInOrder(list, 2, 0, 1);

        list.addOrVote("x");
        list.addOrVote("x");
        list.addOrVote("x");
        assertItemsInOrder(list, "x", "d", "c");
        assertWeightsInOrder(list, 4, 2, 0);
    }

    private <T> void assertItemsInOrder(WeightedEvictableList<String> weightedList, T... values) {
        assertEquals(values.length, weightedList.size());
        for (int i = 0; i < values.length; i++) {
            assertEquals("Item " + i + " at snapshot is not correct", values[i], weightedList.getWeightedItem(i).getItem());
        }
    }

    private <T> void assertWeightsInOrder(WeightedEvictableList<String> weightedList, int... weights) {
        assertEquals(weights.length, weightedList.size());
        for (int i = 0; i < weights.length; i++) {
            assertEquals("Item " + i + " at list does not have correct weight", weights[i], weightedList.getWeightedItem(i).weight);
        }
    }
}
