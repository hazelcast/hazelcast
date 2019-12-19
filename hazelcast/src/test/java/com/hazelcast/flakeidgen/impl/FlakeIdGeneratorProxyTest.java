/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.IdBatchAndWaitTime;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
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
import java.util.UUID;

import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_ALLOWED_FUTURE_MILLIS;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_NODE_ID;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_SEQUENCE;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_TIMESTAMP;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_EPOCH_START;
import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.NODE_ID_UPDATE_INTERVAL_NS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGeneratorProxyTest {

    /**
     * Available number of IDs per second from single member
     */
    private final int IDS_PER_SECOND = 1 << DEFAULT_BITS_SEQUENCE;
    private static final ILogger LOG = Logger.getLogger(FlakeIdGeneratorProxyTest.class);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private FlakeIdGeneratorProxy gen;
    private ClusterService clusterService;

    @Before
    public void before() {
        before(0, 0);
    }

    public void before(long idOffset, long nodeIdOffset) {
        ILogger logger = mock(ILogger.class);
        clusterService = mock(ClusterService.class);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        FlakeIdGeneratorService service = mock(FlakeIdGeneratorService.class);
        when(nodeEngine.getLogger(FlakeIdGeneratorProxy.class)).thenReturn(logger);
        when(nodeEngine.isRunning()).thenReturn(true);
        when(nodeEngine.getConfig()).thenReturn(new Config().addFlakeIdGeneratorConfig(
                new FlakeIdGeneratorConfig("foo").setIdOffset(idOffset).setNodeIdOffset(nodeIdOffset)
        ));
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

    public void test_usedBits() {
        assertEquals(63, DEFAULT_BITS_NODE_ID + DEFAULT_BITS_TIMESTAMP + DEFAULT_BITS_SEQUENCE);
    }

    @Test
    public void test_timeMiddle() {
        IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, 10);
        assertEquals(5300086112257234L, result.idBatch.base());
    }

    @Test
    public void test_timeLowEdge() {
        IdBatchAndWaitTime result = gen.newIdBaseLocal(DEFAULT_EPOCH_START, 1234, 10);
        assertEquals(1234L, result.idBatch.base());
    }

    @Test
    public void test_timeHighEdge() {
        IdBatchAndWaitTime result = gen.newIdBaseLocal(DEFAULT_EPOCH_START + (1L << DEFAULT_BITS_TIMESTAMP) - 1L, 1234, 10);
        assertEquals(9223372036850582738L, result.idBatch.base());
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
    public void when_twoIdsAtTheSameMoment_then_higherSeq() {
        long id1 = gen.newIdBaseLocal(1516028439000L, 1234, 1).idBatch.base();
        long id2 = gen.newIdBaseLocal(1516028439000L, 1234, 1).idBatch.base();
        assertEquals(5300086112257234L, id1);
        assertEquals(id1 + (1 << DEFAULT_BITS_NODE_ID), id2);
    }

    @Test
    public void test_minimumIdOffset() {
        // By assigning MIN_VALUE idOffset we'll offset the default epoch start by (Long.MIN_VALUE >> 22) ms, that is
        // by about 69 years. So the lowest working date will be:
        before(Long.MIN_VALUE, 0);
        long id = gen.newIdBaseLocal(DEFAULT_EPOCH_START, 1234, 1).idBatch.base();
        LOG.info("ID=" + id);
        assertEquals(-9223372036854774574L, id);
    }

    @Test
    public void test_maximumIdOffset() {
        // By assigning MIN_VALUE idOffset we'll offset the default epoch start by (Long.MIN_VALUE >> 22) ms, that is
        // by about 69 years. So the lowest working date will be:
        before(Long.MAX_VALUE, 0);
        long id = gen.newIdBaseLocal(DEFAULT_EPOCH_START, 1234, 1).idBatch.base();
        LOG.info("ID=" + id);
        assertEquals(9223372036850582738L, id);
    }

    @Test
    public void test_positiveNodeIdOffset() {
        int nodeIdOffset = 5;
        int memberListJoinVersion = 20;
        before(0, nodeIdOffset);

        when(clusterService.getMemberListJoinVersion()).thenReturn(memberListJoinVersion);
        assertEquals((memberListJoinVersion + nodeIdOffset), gen.getNodeId(0));
    }

    @Test
    public void when_migrationScenario_then_idFromFlakeIdIsLarger() {
        // This simulates the migration scenario: we take the largest IdGenerator value (a constant here)
        // and the current FIG value. Then we configure the offset based on their difference plus the reserve
        // and check, that the ID after idOffset is set is larger than the value from IG.
        long largestIdGeneratorValue = 5421380884070400000L;
        long currentFlakeGenValue = gen.newId();
        // this number is mentioned in FlakeIdGeneratorConfig.setIdOffset()
        long reserve = 274877906944L;
        // This test will start failing after December 17th 2058 3:52:07 UTC: after this time no idOffset will be needed.
        assertTrue(largestIdGeneratorValue > currentFlakeGenValue);
        // the before() call will create a new gen
        before(largestIdGeneratorValue - currentFlakeGenValue + reserve, 0);

        // Then
        long newFlakeGenValue = gen.newId();
        assertTrue(newFlakeGenValue > largestIdGeneratorValue);
        assertTrue(newFlakeGenValue - largestIdGeneratorValue < MINUTES.toMillis(10) * (1 << (DEFAULT_BITS_NODE_ID + DEFAULT_BITS_SEQUENCE)));
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
    public void when_10mIds_then_wait() {
        int batchSize = 10000000;
        IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, batchSize);
        assertEquals(batchSize / IDS_PER_SECOND - DEFAULT_ALLOWED_FUTURE_MILLIS, result.waitTimeMillis);
    }

    @Test
    public void when_10mIdsInSmallChunks_then_wait() {
        int batchSize = 100;
        for (int numIds = 0; numIds < 10000000; numIds += batchSize) {
            IdBatchAndWaitTime result = gen.newIdBaseLocal(1516028439000L, 1234, batchSize);
            assertEquals(Math.max(0, (numIds + batchSize) / IDS_PER_SECOND - DEFAULT_ALLOWED_FUTURE_MILLIS), result.waitTimeMillis);
        }
    }
}
