/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl.source;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;

import static com.hazelcast.jet.kinesis.impl.source.HashRange.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HashRangeTest {

    @Test
    public void partition() {
        HashRange range = HashRange.range(1000, 2000);
        assertEquals(HashRange.range(1000, 1500), range.partition(0, 2));
        assertEquals(HashRange.range(1500, 2000), range.partition(1, 2));

        range = range("0", "170141183460469231731687303715884105728");
        assertEquals(range("0", "14178431955039102644307275309657008810"),
                range.partition(0, 12));
        assertEquals(range("14178431955039102644307275309657008810", "28356863910078205288614550619314017621"),
                range.partition(1, 12));
        assertEquals(range("28356863910078205288614550619314017621", "42535295865117307932921825928971026432"),
                range.partition(2, 12));
        // ...
        assertEquals(range("141784319550391026443072753096570088106", "155962751505430129087380028406227096917"),
                range.partition(10, 12));
        assertEquals(range("155962751505430129087380028406227096917", "170141183460469231731687303715884105728"),
                range.partition(11, 12));
    }

    @Test
    public void contains() {
        HashRange range = HashRange.range(1000, 2000);
        assertFalse(range.contains(new BigInteger("999")));
        assertTrue(range.contains(new BigInteger("1000")));
        assertTrue(range.contains(new BigInteger("1999")));
        assertFalse(range.contains(new BigInteger("2000")));
        assertFalse(range.contains(new BigInteger("2001")));
    }

    @Test
    public void isAdjacent() {
        HashRange range = HashRange.range(1000, 2000);
        assertFalse(range.isAdjacent(HashRange.range(0, 999)));
        assertTrue(range.isAdjacent(HashRange.range(0, 1000)));
        assertTrue(range.isAdjacent(HashRange.range(2000, 3000)));
        assertFalse(range.isAdjacent(HashRange.range(2001, 3000)));
    }

}
