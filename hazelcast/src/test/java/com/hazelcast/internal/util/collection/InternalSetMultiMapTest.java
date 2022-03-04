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
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalSetMultiMapTest {

    private InternalSetMultimap<Integer, String> multiMap;

    @Before
    public void setUp() {
        this.multiMap = new InternalSetMultimap<Integer, String>();
    }

    @Test
    public void put_whenEmpty_thenInsertItIntoCollection() {
        multiMap.put(1, "value");
        Collection<String> results = multiMap.get(1);

        assertThat(results, hasSize(1));
        assertThat(results, contains("value"));
    }

    @Test
    public void givenKeyHasValueAlreadyAssociated_whenTheSameValueIsAssociatedAgain_thenDuplicateIsEliminated() {
        multiMap.put(1, "value");
        multiMap.put(1, "value");
        multiMap.put(1, "value");

        Collection<String> results = multiMap.get(1);

        assertThat(results, hasSize(1));
        assertThat(results, contains("value"));
    }

    @Test
    public void entrySet_whenEmpty_thenReturnEmptySet() {
        Set<?> entries = multiMap.entrySet();
        assertThat(entries, hasSize(0));
    }
}
