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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.internal.networking.nio.NioInboundPipeline;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.networking.nio.NioOutboundPipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerStressTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overridePropertyRule = OverridePropertyRule.set("hazelcast.io.load", "0");

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testEachConnectionUseDifferentOwnerEventually() {
        Config config = new Config()
                .setProperty(GroupProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1")
                .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "2");

        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        instance2.shutdown();
        instance2 = Hazelcast.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(randomMapName());
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        assertBalanced(instance1);
        assertBalanced(instance2);
        assertBalanced(instance3);
    }

    private void assertBalanced(HazelcastInstance hz) {
        EndpointManager<TcpIpConnection> em = getEndpointManager(hz);

        Map<NioThread, Set<MigratablePipeline>> pipelinesPerOwner = getPipelinesPerOwner(em);

        try {
            for (Map.Entry<NioThread, Set<MigratablePipeline>> entry : pipelinesPerOwner.entrySet()) {
                NioThread owner = entry.getKey();
                Set<MigratablePipeline> pipelines = entry.getValue();
                assertBalanced(owner, pipelines);
            }
        } catch (AssertionError e) {
            // if something fails, we want to see the debug output
            System.out.println(debug(hz));
            throw e;
        }
    }

    private Map<NioThread, Set<MigratablePipeline>> getPipelinesPerOwner(EndpointManager<TcpIpConnection> em) {
        Map<NioThread, Set<MigratablePipeline>> pipelinesPerOwner = new HashMap<NioThread, Set<MigratablePipeline>>();
        for (TcpIpConnection connection : em.getActiveConnections()) {
            NioChannel channel = (NioChannel) connection.getChannel();
            add(pipelinesPerOwner, channel.inboundPipeline());
            add(pipelinesPerOwner, channel.outboundPipeline());
        }
        return pipelinesPerOwner;
    }

    private void add(Map<NioThread, Set<MigratablePipeline>> pipelinesPerOwner, MigratablePipeline pipeline) {
        NioThread pipelineOwner = pipeline.owner();
        Set<MigratablePipeline> pipelines = pipelinesPerOwner.get(pipelineOwner);
        if (pipelines == null) {
            pipelines = new HashSet<MigratablePipeline>();
            pipelinesPerOwner.put(pipelineOwner, pipelines);
        }
        pipelines.add(pipeline);
    }

    /**
     * A owner is balanced if:
     * <ul>
     * <li>it has 1 active handler (so a high event count)</li>
     * <li>potentially several dead handlers (duplicate connection), on which event counts should be low</li>
     * </ul>
     */
    private void assertBalanced(NioThread owner, Set<MigratablePipeline> pipelines) {
        assertTrue("no pipelines were found for owner:" + owner, pipelines.size() > 0);

        MigratablePipeline[] pipelinesArr = pipelines.toArray(new MigratablePipeline[0]);
        Arrays.sort(pipelinesArr, new PipelineLoadComparator());

        MigratablePipeline activePipeline = pipelinesArr[pipelinesArr.length - 1];
        assertTrue("at least 1000 events should have been received by the active pipeline but was:"
                + activePipeline.load(), activePipeline.load() > 1000);

        if (pipelinesArr.length > 1) {
            // owning thread has some dead pipelines
            for (int i = 0; i < pipelinesArr.length - 1; i++) {
                MigratablePipeline deadPipeline = pipelinesArr[i];

                // the maximum number of events seen on a dead connection is 3.
                // we assert that there are less than 10 just to be on the safe side
                assertTrue("a dead pipeline at most 10 event should have been received, number of events received:"
                        + deadPipeline.load(), deadPipeline.load() < 10);
            }
        }
    }

    private String debug(HazelcastInstance hz) {
        NioNetworking threadingModel = (NioNetworking) getNode(hz).getNetworkingService().getNetworking();
        EndpointManager<TcpIpConnection> em = getNode(hz).getEndpointManager();

        StringBuilder sb = new StringBuilder();
        sb.append("in owners\n");
        for (NioThread in : threadingModel.getInputThreads()) {
            sb.append(in).append(": ").append(in.getEventCount()).append("\n");

            for (TcpIpConnection connection : em.getActiveConnections()) {
                NioInboundPipeline socketReader = ((NioChannel) connection.getChannel()).inboundPipeline();
                if (socketReader.owner() == in) {
                    sb.append("\t").append(socketReader).append(" load:").append(socketReader.load()).append("\n");
                }
            }
        }
        sb.append("out owners\n");
        for (NioThread in : threadingModel.getOutputThreads()) {
            sb.append(in).append(": ").append(in.getEventCount()).append("\n");

            for (TcpIpConnection connection : em.getActiveConnections()) {
                NioOutboundPipeline socketWriter = ((NioChannel) connection.getChannel()).outboundPipeline();
                if (socketWriter.owner() == in) {
                    sb.append("\t").append(socketWriter).append(" load:").append(socketWriter.load()).append("\n");
                }
            }
        }

        return sb.toString();
    }

    private static class PipelineLoadComparator implements Comparator<MigratablePipeline> {
        @Override
        public int compare(MigratablePipeline pipeline1, MigratablePipeline pipeline2) {
            final long l1 = pipeline1.load();
            final long l2 = pipeline2.load();
            return (l1 < l2) ? -1 : ((l1 == l2) ? 0 : 1);
        }
    }
}
