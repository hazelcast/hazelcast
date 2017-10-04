/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector.kafka;

import com.hazelcast.jet.impl.connector.kafka.StreamKafkaP.KafkaPartitionAssigner;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class KafkaPartitionAssignerTest {

    private KafkaPartitionAssigner assigner;

    @Test
    public void when_singleTopicMultiplePartitions() throws Exception {
        assigner = assigner(4, 16);
        assertAssignment(0,
                tp(0, 0),
                tp(0, 4),
                tp(0, 8),
                tp(0, 12)
        );
        assertAssignment(1,
                tp(0, 1),
                tp(0, 5),
                tp(0, 9),
                tp(0, 13)
        );
        assertAssignment(2,
                tp(0, 2),
                tp(0, 6),
                tp(0, 10),
                tp(0, 14)
        );
        assertAssignment(3,
                tp(0, 3),
                tp(0, 7),
                tp(0, 11),
                tp(0, 15)
        );
    }

    @Test
    public void when_multipleTopic_multiplePartitions() throws Exception {
        assigner = assigner(8, 4, 4);

        assertAssignment(0, tp(0, 0));
        assertAssignment(1, tp(0, 1));
        assertAssignment(2, tp(0, 2));
        assertAssignment(3, tp(0, 3));
        assertAssignment(4, tp(1, 0));
        assertAssignment(5, tp(1, 1));
        assertAssignment(6, tp(1, 2));
        assertAssignment(7, tp(1, 3));
    }

    @Test
    public void when_singleProcessor() throws Exception {
        assigner = assigner(1, 4, 4);

        assertAssignment(0,
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
    public void when_multipleTopicsWithSinglePartition() throws Exception {
        assigner = assigner(4, 1, 1, 1, 1);

        assertAssignment(0, tp(0, 0));
        assertAssignment(1, tp(1, 0));
        assertAssignment(2, tp(2, 0));
        assertAssignment(3, tp(3, 0));
    }

    @Test
    public void when_moreTopicsThanParallelism() throws Exception {
        assigner = assigner(2, 1, 1, 1, 1);

        assertAssignment(0,
                tp(0, 0),
                tp(2, 0)
        );
        assertAssignment(1,
                tp(1, 0),
                tp(3, 0)
        );

    }

    private void assertAssignment(int processorIdx, TopicPartition... partitions) {
        assertEquals(new HashSet<>(asList(partitions)), assigner.topicPartitionsFor(processorIdx));
    }

    private static TopicPartition tp(int topicIndex, int partition) {
        return new TopicPartition("topic-" + topicIndex, partition);
    }

    private static KafkaPartitionAssigner assigner(int globalParallelism, int... partitionCounts) {
        List<String> topics = IntStream.range(0, partitionCounts.length).mapToObj(i -> "topic-" + i).collect(toList());
        List<Integer> counts = IntStream.of(partitionCounts).boxed().collect(Collectors.toList());
        return new KafkaPartitionAssigner(topics, counts, globalParallelism);
    }
}
