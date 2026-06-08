/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.service;

import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorValues;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorCollectionSplitBrainProtectionReadTest extends AbstractVectorCollectionSplitBrainProtectionTest {

    @Parameterized.Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.READ}, {SplitBrainProtectionOn.READ_WRITE}});
    }

    private VectorCollection<String, String> collection;

    @Test
    public void getAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.getAsync("any")).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void getAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.getAsync("any")).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void searchAsync_whenSplitBrainProtectionReqsMatch() throws ExecutionException, InterruptedException {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.searchAsync(VectorValues.of(new float[] {1.0f, 1.0f}), SearchOptions.of(3, false, false)))
                .succeedsWithin(TIMEOUT).extracting(SearchResults::size).isEqualTo(3);
    }

    @Test
    public void searchAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.searchAsync(VectorValues.of(new float[] {1.0f, 1.0f}), SearchOptions.of(3, false, false)))
                .failsWithin(TIMEOUT).withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void size_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.size()).isPositive();
    }

    @Test
    public void size_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThatThrownBy(() -> collection.size()).isInstanceOf(SplitBrainProtectionException.class);
    }
}
