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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsDictionaryTest {
    @Test
    public void testGrowing() {
        MetricsDictionary dictionary = new MetricsDictionary();
        for (int i = 0; i < 256; i++) {
            String word = Integer.toString(i);
            int dictionaryId = dictionary.getDictionaryId(word);
            assertEquals(i, dictionaryId);
            assertEquals(word, dictionary.get(i));

            String foundWord = dictionary.get(dictionaryId);
            assertEquals(word, foundWord);

            assertNull(dictionary.get(i + 1));
            assertEquals(i + 1, dictionary.size());
        }
    }

    @Test
    public void testGetWithNegativeIdReturnsNull() {
        MetricsDictionary dictionary = new MetricsDictionary();
        assertNull(dictionary.get(-1));
    }

    @Test
    public void testGetWithNonExistentIdReturnsNull() {
        MetricsDictionary dictionary = new MetricsDictionary();
        assertNull(dictionary.get(Integer.MAX_VALUE));
    }

    @Test
    public void testGetDictionaryIdReturnsSameIdForSameWord() {
        MetricsDictionary dictionary = new MetricsDictionary();
        int word1Id = dictionary.getDictionaryId("word1");
        dictionary.getDictionaryId("word2");
        dictionary.getDictionaryId("word3");
        dictionary.getDictionaryId("word4");

        assertEquals(word1Id, dictionary.getDictionaryId("word1"));
    }

}
