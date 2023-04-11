/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SourceOffsetStorageReaderTest {
    @Test
    public void should_return_null_offset_for_non_existing_partition() {
        Map<String, String> partition = mapOf("part1", "something");
        State state = new State();
        SourceOffsetStorageReader sut = new SourceOffsetStorageReader(state);
        Map<String, Object> offset = sut.offset(partition);
        assertThat(offset).isNull();
    }

    @Test
    public void should_return_offset_for_existing_partition() {
        Map<String, String> partition = mapOf("part1", "something");
        Map<Map<String, ?>, Map<String, ?>> partitionToOffset = mapOf(partition, mapOf("part1", 123));
        State state = new State(partitionToOffset);
        SourceOffsetStorageReader sut = new SourceOffsetStorageReader(state);
        Map<String, Object> offset = sut.offset(partition);
        assertThat(offset).isEqualTo(mapOf("part1", 123));
    }

    @Nonnull
    private static <K, V> Map<K, V> mapOf(K key, V value) {
        Map<K, V> partition = new HashMap<>();
        partition.put(key, value);
        return partition;
    }
}
