/*
 * Copyright 2026 Hazelcast Inc.
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
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.util.Date;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InitialShardIteratorsTest {

    private static final String STREAM = "stream";

    private static final Shard SHARD0 = shard("shard0", 0L, 500L);
    private static final Shard SHARD1 = shard("shard1", 500L, 1000L);
    private static final Shard SHARD2 = shard("shard2", 1000L, 1500L);
    private static final Shard SHARD3 = shard("shard3", 1500L, 2000L);

    private final InitialShardIterators iterators = new InitialShardIterators();

    @Test
    public void unspecified() {
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD3.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber("1500")
                .build(),
                iterators.request(STREAM, SHARD3)
        );
    }

    @Test
    public void latest() {
        iterators.add(".*", ShardIteratorType.LATEST.name(), null);
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD0.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.LATEST)
                .build(),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void oldest() {
        iterators.add(".*", ShardIteratorType.TRIM_HORIZON.name(), null);
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD0.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build(),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void timestamp() {
        long currentTimeMillis = System.currentTimeMillis();
        iterators.add(".*", ShardIteratorType.AT_TIMESTAMP.name(), Long.toString(currentTimeMillis));
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD0.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                        .timestamp(new Date(currentTimeMillis).toInstant())
                .build(),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void atSequence() {
        String seqNo = "12345";
        iterators.add(".*", ShardIteratorType.AT_SEQUENCE_NUMBER.name(), seqNo);
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD0.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber(seqNo)
                .build(),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void afterSequence() {
        String seqNo = "12345";
        iterators.add(".*", ShardIteratorType.AFTER_SEQUENCE_NUMBER.name(), seqNo);
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD0.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                        .startingSequenceNumber(seqNo)
                .build(),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void sequentialCheck() {
        iterators.add(SHARD1.shardId(), ShardIteratorType.LATEST.name(), null);
        iterators.add(SHARD3.shardId(), ShardIteratorType.TRIM_HORIZON.name(), null);
        iterators.add(".*", ShardIteratorType.AT_SEQUENCE_NUMBER.name(), "9999");

        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD2.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber("9999")
                .build(),
                iterators.request(STREAM, SHARD2)
        ); //matches last
        assertEquals(
                GetShardIteratorRequest.builder()
                        .shardId(SHARD1.shardId())
                        .streamName(STREAM)
                        .shardIteratorType(ShardIteratorType.LATEST)
                .build(),
                iterators.request(STREAM, SHARD1)
        ); //matches first
    }

    private static Shard shard(String id, long startHashKey, long endHashKey) {
        SequenceNumberRange sequenceNumberRange = SequenceNumberRange.builder()
                .startingSequenceNumber(Long.toString(startHashKey))
                .endingSequenceNumber(Long.toString(endHashKey))
                .build();
        return Shard.builder().shardId(id).sequenceNumberRange(sequenceNumberRange)
                .build();
    }

}
