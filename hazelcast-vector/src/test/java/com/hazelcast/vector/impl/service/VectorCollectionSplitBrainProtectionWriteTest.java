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
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class VectorCollectionSplitBrainProtectionWriteTest extends AbstractVectorCollectionSplitBrainProtectionTest {

    @Parameterized.Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.WRITE}, {SplitBrainProtectionOn.READ_WRITE}});
    }

    private VectorCollection<String, String> collection;

    @Test
    public void putAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.putAsync(randomString(), randomDoc())).succeedsWithin(TIMEOUT);
    }

    @Test
    public void putAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.putAsync(randomString(), randomDoc())).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void putAllAsync_whenSplitBrainProtectionReqsMatch() {
        Map<String, VectorDocument<String>> putAllMap = getMapWithMultipleKeysOnSamePartition(0);
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.putAllAsync(putAllMap)).succeedsWithin(TIMEOUT);
    }

    @Test
    public void putAllAsync_whenSplitBrainProtectionReqsDontMatch() {
        Map<String, VectorDocument<String>> putAllMap = getMapWithMultipleKeysOnSamePartition(3);
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.putAllAsync(putAllMap))
                .failsWithin(TIMEOUT).withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void putIfAbsentAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.putIfAbsentAsync(randomString(), randomDoc())).succeedsWithin(TIMEOUT);
    }

    @Test
    public void putIfAbsentAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.putIfAbsentAsync(randomString(), randomDoc()))
                .failsWithin(TIMEOUT).withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void setAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.setAsync(randomString(), randomDoc())).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void setAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.setAsync(randomString(), randomDoc())).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void clearAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.clearAsync()).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void clearAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.clearAsync()).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void deleteAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.deleteAsync(randomString())).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void deleteAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.deleteAsync(randomString())).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void removeAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.removeAsync(randomString())).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void removeAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.removeAsync(randomString())).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    @Test
    public void optimizeAsync_whenSplitBrainProtectionReqsMatch() {
        collection = vectorCollection(0, splitBrainProtectionOn);
        assertThat(collection.optimizeAsync()).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void optimizeAsync_whenSplitBrainProtectionReqsDontMatch() {
        collection = vectorCollection(3, splitBrainProtectionOn);
        assertThat(collection.optimizeAsync()).failsWithin(TIMEOUT)
                .withThrowableThat().withCauseInstanceOf(SplitBrainProtectionException.class);
    }

    VectorDocument<String> randomDoc()  {
        return VectorDocument.of(randomString(), randomVec(2));
    }

    // Generates a map of key-documents that belong to the same partition. Important for testing
    // putAllAsync from client-side, since it will send a set client message instead of putAll
    // when there is a single entry in a partition.
    private Map<String, VectorDocument<String>> getMapWithMultipleKeysOnSamePartition(int instanceIndex) {
        String[] keys = generateKeysBelongingToSamePartitionsOwnedBy(cluster.getInstance(instanceIndex), 3);
        Map<String, VectorDocument<String>> putAllMap = new HashMap<>();
        for (String k : keys) {
            putAllMap.put(k, randomDoc());
        }
        return putAllMap;
    }
}
