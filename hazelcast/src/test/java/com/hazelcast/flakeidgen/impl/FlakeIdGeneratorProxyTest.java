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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.IdBatchAndWaitTime;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_ALLOWED_FUTURE_MILLIS;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_NODE_ID;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_SEQUENCE;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_EPOCH_START;
import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.NODE_ID_UPDATE_INTERVAL_NS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGeneratorProxyTest {

    /**
     * Available number of IDs per second from a single member
     */
    private final int IDS_PER_SECOND = 1 << DEFAULT_BITS_SEQUENCE;
    private static final int MAX_BIT_LENGTH = 63;
    private final long DEFAULT_BITS_TIMESTAMP = MAX_BIT_LENGTH - (DEFAULT_BITS_NODE_ID + DEFAULT_BITS_SEQUENCE);
    private static final ILogger LOG = Logger.getLogger(FlakeIdGeneratorProxyTest.class);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private FlakeIdGeneratorProxy gen;
    private ClusterService clusterService;

    @Before
    public void before() {
        initialize(new FlakeIdGeneratorConfig());
    }

    public void initialize(FlakeIdGeneratorConfig config) {
        ILogger logger = mock(ILogger.class);
        clusterService = mock(ClusterService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        FlakeIdGeneratorService service = mock(FlakeIdGeneratorService.class);
        when(nodeEngine.getLogger(FlakeIdGeneratorProxy.class)).thenReturn(logger);
        when(nodeEngine.isRunning()).thenReturn(true);
        config.setName("foo");
        when(nodeEngine.getConfig()).thenReturn(new Config().addFlakeIdGeneratorConfig(config));
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Address address = null;
        try {
            address = new Address("127.0.0.1", 5701);
        } catch (UnknownHostException e) {
            // no-op
        }
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl(address, MemberVersion.UNKNOWN, true, UUID.randomUUID()));
        UUID source = nodeEngine.getLocalMember().getUuid();
        gen = new FlakeIdGeneratorProxy("foo", nodeEngine, service, source);
    }

    @Test
    public void when_nodeIdUpdated_then_pickedUpAfterUpdateInterval() {
        when(clusterService.getMemberListJoinVersion()).thenReturn(20);
        assertEquals(20, gen.getNodeId(0));

        when(clusterService.getMemberListJoinVersion()).thenReturn(30);
        assertEquals(20, gen.getNodeId(0));
        assertEquals(20, gen.getNodeId(NODE_ID_UPDATE_INTERVAL_NS - 1));
        assertEquals(30, gen.getNodeId(NODE_ID_UPDATE_INTERVAL_NS));
    }

    @Test
    public void givenNodeIdUninitialized_whenNodeIdRequestedConcurrently_thenItNeverReturnUninitializedId() throws Exception {
        when(clusterService.getMemberListJoinVersion()).thenReturn(20);
        int threadCount = 20;
        int iterationCount = 5_000;

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        AtomicInteger errorCounter = new AtomicInteger();
        FlakeIdGeneratorConfig genConfig = new FlakeIdGeneratorConfig();
        for (int i = 0; i < iterationCount; i++) {
            initialize(genConfig);
            FlakeIdGeneratorProxy localGen = gen;
            Runnable getNodeId = () -> {
                if (localGen.getNodeId(0) == -1) { // see FlakeIdGeneratorProxy#NODE_ID_NOT_YET_SET
                    errorCounter.incrementAndGet();
                }
            };
            for (int z = 0; z < threadCount; z++) {
                executorService.submit(getNodeId);
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(30, SECONDS);
        assertEquals(0, errorCounter.get());
    }

    @Test
    public void test_timeLowPositiveEdge() {
        long id = gen.newIdBaseLocal(DEFAULT_EPOCH_START, 1234, 10).idBatch.base();
        assertEquals(1234L, id);
    }

    @Test
    public void test_timeMiddle() {
        long id = gen.newIdBaseLocal(1516028439000L, 1234, 10).idBatch.base();
        assertEquals(5300086112257234L, id);
    }

    @Test
    public void test_timeHighEdge() {
        IdBatchAndWaitTime result = gen.newIdBaseLocal(DEFAULT_EPOCH_START + (1L << DEFAULT_BITS_TIMESTAMP) - 1L, 1234, 10);
        assertEquals(9223372036850582738L, result.idBatch.base());
    }

    @Test
    public void test_negativeId() {
        long id = gen.newIdBaseLocal(DEFAULT_EPOCH_START - 1, 1234, 10).idBatch.base();
        assertEquals((-1 << DEFAULT_BITS_SEQUENCE + DEFAULT_BITS_NODE_ID) + 1234, id);
    }

    @Test
    public void test_lowNegativeEdge() {
        long timestamp = -(1L << DEFAULT_BITS_TIMESTAMP);
        long id = gen.newIdBaseLocal(DEFAULT_EPOCH_START + timestamp, 1234, 10).idBatch.base();
        assertEquals(Long.MIN_VALUE + 1234, id);
    }

    @Test
    public void test_idsOrdered() {
        long lastId = -1;
        for (long now = DEFAULT_EPOCH_START;
             now < DEFAULT_EPOCH_START + (1L << DEFAULT_BITS_TIMESTAMP);
             now += 365L * 24L * 60L * 60L * 1000L) {
            long base = gen.newIdBaseLocal(now, 1234, 1).idBatch.base();
            LOG.info("at " + new Date(now) + ", id=" + base);
            assertTrue("lastId=" + lastId + ", newId=" + base, lastId < base);
            lastId = base;
        }
    }

    @Test
    public void when_currentTimeBeforeAllowedRange_then_fail() {
        long lowestGoodTimestamp = DEFAULT_EPOCH_START - (1L << DEFAULT_BITS_TIMESTAMP);
        gen.newIdBaseLocal(lowestGoodTimestamp, 0, 1);
        exception.expect(HazelcastException.class);
        exception.expectMessage("Current time out of allowed range");
        gen.newIdBaseLocal(lowestGoodTimestamp - 1, 0, 1);
    }

    @Test
    public void when_currentTimeAfterAllowedRange_then_fail() {
        gen.newIdBaseLocal(DEFAULT_EPOCH_START + (1L << DEFAULT_BITS_TIMESTAMP) - 1, 0, 1);
        exception.expect(HazelcastException.class);
        exception.expectMessage("Current time out of allowed range");
        gen.newIdBaseLocal(DEFAULT_EPOCH_START + (1L << DEFAULT_BITS_TIMESTAMP), 0, 1);
    }

    @Test
    public void when_twoIdsAtTheSameMoment_then_higherSeq() {
        long id1 = gen.newIdBaseLocal(1516028439000L, 1234, 1).idBatch.base();
        long id2 = gen.newIdBaseLocal(1516028439000L, 1234, 1).idBatch.base();
        assertEquals(5300086112257234L, id1);
        assertEquals(id1 + (1 << DEFAULT_BITS_NODE_ID), id2);
    }

    @Test
    public void test_positiveNodeIdOffset() {
        int nodeIdOffset = 5;
        int memberListJoinVersion = 20;
        initialize(new FlakeIdGeneratorConfig().setNodeIdOffset(nodeIdOffset));

        when(clusterService.getMemberListJoinVersion()).thenReturn(memberListJoinVersion);
        assertEquals((memberListJoinVersion + nodeIdOffset), gen.getNodeId(0));
    }

    @Test
    public void when_customBits_then_used() {
        int bitsSequence = 10;
        int bitsNodeId = 11;
        initialize(new FlakeIdGeneratorConfig()
                .setBitsSequence(bitsSequence)
                .setBitsNodeId(bitsNodeId)
                .setEpochStart(0));
        Iterator<Long> result = gen.newIdBaseLocal(1, 1234, 2).idBatch.iterator();
        long expected = (1L << bitsSequence + bitsNodeId) + 1234;
        assertEquals(expected, result.next().longValue());
        expected += 1L << bitsNodeId;
        assertEquals(expected, result.next().longValue());
    }

    @Test
    public void when_epochStart_then_used() {
        int epochStart = 456;
        int timeSinceEpochStart = 1;
        initialize(new FlakeIdGeneratorConfig().setEpochStart(epochStart));
        long id = gen.newIdBaseLocal(epochStart + timeSinceEpochStart, 1234, 10).idBatch.base();
        assertEquals((timeSinceEpochStart << DEFAULT_BITS_SEQUENCE + DEFAULT_BITS_NODE_ID) + 1234, id);
    }

    // #### Tests pertaining to wait time ####

    @Test
    public void when_fewIds_then_noWaitTime() {
        assertEquals(0, gen.newIdBaseLocal(1516028439000L, 1234, 100).waitTimeMillis);
    }

    @Test
    public void when_maximumAllowedFuture_then_noWaitTime() {
        IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, (int) (IDS_PER_SECOND * DEFAULT_ALLOWED_FUTURE_MILLIS));
        assertEquals(0, result.waitTimeMillis);
    }

    @Test
    public void when_maximumAllowedFuturePlusOne_then_1msWaitTime() {
        int batchSize = (int) (IDS_PER_SECOND * DEFAULT_ALLOWED_FUTURE_MILLIS) + IDS_PER_SECOND;
        IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, batchSize);
        assertEquals(1, result.waitTimeMillis);
    }

    @Test
    public void when_10mIdsInOneBatch_then_wait() {
        int batchSize = 10_000_000;
        IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, batchSize);
        assertEquals(batchSize / IDS_PER_SECOND - DEFAULT_ALLOWED_FUTURE_MILLIS, result.waitTimeMillis);
    }

    @Test
    public void when_10mIdsInSmallBatches_then_wait() {
        int batchSize = 1000;
        for (int numIds = 0; numIds < 10_000_000; numIds += batchSize) {
            IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, batchSize);
            assertEquals(Math.max(0, (numIds + batchSize) / IDS_PER_SECOND - DEFAULT_ALLOWED_FUTURE_MILLIS), result.waitTimeMillis);
        }
    }
}
