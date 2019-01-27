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

package com.hazelcast.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.collection.WeightedEvictableList.WeightedItem;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

@Category({SlowTest.class, ParallelTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class WeightedEvictableListTest {

    @Test
    public void testGetVote() {
        WeightedEvictableList<String> list = new WeightedEvictableList<String>(3, 3);
        list.add("a");
        list.add("b");
        list.add("c");

        List<WeightedItem<String>> snapshot = list.getList();
        assertSnapshot(snapshot, "a", "b", "c");

        list.voteFor(snapshot.get(2));
        list.voteFor(snapshot.get(1));
        list.voteFor(snapshot.get(2));
        // votes so far a: 0, b: 1, c: 2; 3 iterations are here, now list will organize itself
        snapshot = list.getList();
        assertSnapshot(snapshot, "c", "b", "a");

        // votes are 0, 0, 0
        list.voteFor(snapshot.get(1));
        list.voteFor(snapshot.get(0));
        // votes a:0, b:1, c:1
        list.add("d");
        // "a" should go because it has 0 votes

        snapshot = list.getList();
        assertSnapshot(snapshot, "c", "d");

        list.add("x");
        snapshot = list.getList();
        assertSnapshot(snapshot, "c", "d", "x");
        list.voteFor(snapshot.get(2));
        list.voteFor(snapshot.get(2));
        list.voteFor(snapshot.get(2));
        snapshot = list.getList();
        assertSnapshot(snapshot, "x", "c", "d");
    }

    private <T> void assertSnapshot(List<WeightedItem<T>> snapshot, T... values) {
        for (int i = 0; i < values.length; i++) {
            assertEquals("Item " + i + " at snapshot is not correct", values[i], snapshot.get(i).getItem());
        }
    }
}
