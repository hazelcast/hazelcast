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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.impl.MetricsDictionary.Word;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsDictionaryTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private MetricsDictionary dictionary = new MetricsDictionary();

    @Test
    public void testGrowing() {
        for (int i = 0; i < 256; i++) {
            String word = Integer.toString(i);
            int dictionaryId = dictionary.getDictionaryId(word);
            assertEquals(i, dictionaryId);
        }
    }

    @Test
    public void testWordsOrdered() {
        assertEquals(0, dictionary.getDictionaryId("b"));
        assertEquals(1, dictionary.getDictionaryId("a"));

        Iterator<Word> iterator = dictionary.words().iterator();
        assertEquals("a", iterator.next().word());
        assertEquals("b", iterator.next().word());
    }

    @Test
    public void testGetDictionaryIdReturnsSameIdForSameWord() {
        int word1Id = dictionary.getDictionaryId("word1");
        dictionary.getDictionaryId("word2");
        dictionary.getDictionaryId("word3");
        dictionary.getDictionaryId("word4");

        assertEquals(word1Id, dictionary.getDictionaryId("word1"));
    }

    @Test
    public void when_tooLongWord_then_fails() {
        String longWord = Stream.generate(() -> "a")
                                .limit(MetricsDictionary.MAX_WORD_LENGTH + 1)
                                .collect(Collectors.joining());
        exception.expect(LongWordException.class);
        dictionary.getDictionaryId(longWord);
    }
}
