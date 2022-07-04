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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class KafkaPartitionAssignmentTest {

    @Test
    public void when_singleTopicMultiplePartitions() {
        assertAssignment(new int[]{16}, 0, 4,
                tp(0, 0),
                tp(0, 4),
                tp(0, 8),
                tp(0, 12)
        );
        assertAssignment(new int[]{16}, 1, 4,
                tp(0, 1),
                tp(0, 5),
                tp(0, 9),
                tp(0, 13)
        );
        assertAssignment(new int[]{16}, 2, 4,
                tp(0, 2),
                tp(0, 6),
                tp(0, 10),
                tp(0, 14)
        );
        assertAssignment(new int[]{16}, 3, 4,
                tp(0, 3),
                tp(0, 7),
                tp(0, 11),
                tp(0, 15)
        );
    }

    @Test
    public void when_multipleTopic_multiplePartitions() {
        assertAssignment(new int[]{4, 4}, 0, 8, tp(0, 0));
        assertAssignment(new int[]{4, 4}, 1, 8, tp(0, 1));
        assertAssignment(new int[]{4, 4}, 2, 8, tp(0, 2));
        assertAssignment(new int[]{4, 4}, 3, 8, tp(0, 3));
        assertAssignment(new int[]{4, 4}, 4, 8, tp(1, 0));
        assertAssignment(new int[]{4, 4}, 5, 8, tp(1, 1));
        assertAssignment(new int[]{4, 4}, 6, 8, tp(1, 2));
        assertAssignment(new int[]{4, 4}, 7, 8, tp(1, 3));
    }

    @Test
    public void when_singleProcessor() {
        assertAssignment(new int[]{4, 4}, 0, 1,
                tp(0, 0),
                tp(0, 1),
                tp(0, 2),
                tp(0, 3),
                tp(1, 0),
                tp(1, 1),
                tp(1, 2),
                tp(1, 3)
        );
    }

    @Test
    public void when_multipleTopicsWithSinglePartition() {
        assertAssignment(new int[]{1, 1, 1, 1}, 0, 4, tp(0, 0));
        assertAssignment(new int[]{1, 1, 1, 1}, 1, 4, tp(1, 0));
        assertAssignment(new int[]{1, 1, 1, 1}, 2, 4, tp(2, 0));
        assertAssignment(new int[]{1, 1, 1, 1}, 3, 4, tp(3, 0));
    }

    @Test
    public void when_moreTopicsThanParallelism() {
        assertAssignment(new int[]{1, 1, 1, 1}, 0, 2,
                tp(0, 0),
                tp(2, 0)
        );
        assertAssignment(new int[]{1, 1, 1, 1}, 1, 2,
                tp(1, 0),
                tp(3, 0)
        );

    }

    private static void assertAssignment(
            int[] partitionCounts, int processorIdx, int totalParallelism,
            Tuple2<Integer, Integer> ... expectedAssignment
    ) {
        Set<Tuple2<Integer, Integer>> actualAssignment = new HashSet<>();
        for (int topicIdx = 0; topicIdx < partitionCounts.length; topicIdx++) {
            for (int partition = 0; partition < partitionCounts[topicIdx]; partition++) {
                if (StreamKafkaP.handledByThisProcessor(
                        totalParallelism, partitionCounts.length, processorIdx, topicIdx, partition)) {
                    actualAssignment.add(tp(topicIdx, partition));
                }
            }
        }

        assertEquals(new HashSet<>(asList(expectedAssignment)), actualAssignment);
    }

    private static Tuple2<Integer, Integer> tp(int topicIndex, int partition) {
        return tuple2(topicIndex, partition);
    }
}
