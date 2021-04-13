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

import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
                new GetShardIteratorRequest()
                        .withShardId(SHARD3.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .withStartingSequenceNumber("1500"),
                iterators.request(STREAM, SHARD3)
        );
    }

    @Test
    public void latest() {
        iterators.add(".*", ShardIteratorType.LATEST.name(), null);
        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD0.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.LATEST),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void oldest() {
        iterators.add(".*", ShardIteratorType.TRIM_HORIZON.name(), null);
        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD0.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.TRIM_HORIZON),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void timestamp() {
        long currentTimeMillis = System.currentTimeMillis();
        iterators.add(".*", ShardIteratorType.AT_TIMESTAMP.name(), Long.toString(currentTimeMillis));
        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD0.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                        .withTimestamp(new Date(currentTimeMillis)),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void atSequence() {
        String seqNo = "12345";
        iterators.add(".*", ShardIteratorType.AT_SEQUENCE_NUMBER.name(), seqNo);
        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD0.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .withStartingSequenceNumber(seqNo),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void afterSequence() {
        String seqNo = "12345";
        iterators.add(".*", ShardIteratorType.AFTER_SEQUENCE_NUMBER.name(), seqNo);
        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD0.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                        .withStartingSequenceNumber(seqNo),
                iterators.request(STREAM, SHARD0)
        );
    }

    @Test
    public void sequentialCheck() {
        iterators.add(SHARD1.getShardId(), ShardIteratorType.LATEST.name(), null);
        iterators.add(SHARD3.getShardId(), ShardIteratorType.TRIM_HORIZON.name(), null);
        iterators.add(".*", ShardIteratorType.AT_SEQUENCE_NUMBER.name(), "9999");

        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD2.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .withStartingSequenceNumber("9999"),
                iterators.request(STREAM, SHARD2)
        ); //matches last
        assertEquals(
                new GetShardIteratorRequest()
                        .withShardId(SHARD1.getShardId())
                        .withStreamName(STREAM)
                        .withShardIteratorType(ShardIteratorType.LATEST),
                iterators.request(STREAM, SHARD1)
        ); //matches first
    }

    private static Shard shard(String id, long startHashKey, long endHashKey) {
        SequenceNumberRange sequenceNumberRange = new SequenceNumberRange()
                .withStartingSequenceNumber(Long.toString(startHashKey))
                .withEndingSequenceNumber(Long.toString(endHashKey));
        return new Shard().withShardId(id).withSequenceNumberRange(sequenceNumberRange);
    }

}
