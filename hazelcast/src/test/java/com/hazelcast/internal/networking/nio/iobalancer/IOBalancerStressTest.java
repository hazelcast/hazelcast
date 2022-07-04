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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.internal.networking.nio.NioInboundPipeline;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.networking.nio.NioOutboundPipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServer;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.test.Accessors.getConnectionManager;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.stream.Collectors.toMap;
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
                .setProperty(ClusterProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1")
                // for 3 members cluster, it is possible to be maximum of 4 connections per instance with duplicates
                // IOBalancer should rebalance them equally between threads
                .setProperty(ClusterProperty.IO_THREAD_COUNT.getName(), "4");

        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        instance2.shutdown();
        instance2 = Hazelcast.newHazelcastInstance(config);

        // prerecord pipelines load, grouped by the owner-thread, before start the load
        Map<NioThread, Map<MigratablePipeline, Long>>
                pipelinesLoadPerOwnerBeforeLoad1 = getPipelinesLoadPerOwner(instance1);
        Map<NioThread, Map<MigratablePipeline, Long>>
                pipelinesLoadPerOwnerBeforeLoad2 = getPipelinesLoadPerOwner(instance2);
        Map<NioThread, Map<MigratablePipeline, Long>>
                pipelinesLoadPerOwnerBeforeLoad3 = getPipelinesLoadPerOwner(instance3);

        IMap<Integer, Integer> map = instance1.getMap(randomMapName());
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        assertBalanced(pipelinesLoadPerOwnerBeforeLoad1, instance1);
        assertBalanced(pipelinesLoadPerOwnerBeforeLoad2, instance2);
        assertBalanced(pipelinesLoadPerOwnerBeforeLoad3, instance3);
    }

    /**
     * An owner thread is balanced if it has no more than one "active" pipeline
     * and several non-active pipelines
     */
    private void assertBalanced(
            Map<NioThread, Map<MigratablePipeline, Long>> pipelinesLoadPerOwnerBeforeLoad,
            HazelcastInstance hz) {
        // get pipelines load, grouped by the owner-thread, after the load
        Map<NioThread, Map<MigratablePipeline, Long>> pipelinesLoadPerOwnerAfterLoad = getPipelinesLoadPerOwner(hz);
        Map<MigratablePipeline, Long> pipelinesLoadBeforeLoad = getPipelinesLoad(pipelinesLoadPerOwnerBeforeLoad);

        try {
            for (Map.Entry<NioThread, Map<MigratablePipeline, Long>>
                    entry : pipelinesLoadPerOwnerAfterLoad.entrySet()) {
                NioThread owner = entry.getKey();
                Map<MigratablePipeline, Long> pipelinesLoad = entry.getValue();

                // IOBalancer rebalance only "active" pipelines that have some additional load between checks
                // In a case of a pipeline with no new load, it will be skipped by IOBalancer
                if (pipelinesLoad.size() > 1) {
                    int activePipelines = 0;
                    for (Map.Entry<MigratablePipeline, Long> pipelineEntry : pipelinesLoad.entrySet()) {
                        long loadAfterLoad = pipelineEntry.getValue();
                        long loadBeforeLoad = pipelinesLoadBeforeLoad.get(pipelineEntry.getKey());

                        // If the load value for the same pipeline has changed before and after the load, we count it
                        // as an active pipeline, if the load value hasn't changed - we skip it
                        if (loadBeforeLoad < loadAfterLoad) {
                            activePipelines++;
                        }
                    }
                    // Selector threads should have no more than one "active" pipeline
                    assertTrue("The number of active pipelines for the owner " + owner + " is: " + activePipelines,
                            activePipelines <= 1);
                }
            }
        } catch (AssertionError e) {
            // if something fails, we want to see the debug output
            System.out.println(debug(hz, pipelinesLoadPerOwnerBeforeLoad));
            throw e;
        }
    }

    private Map<MigratablePipeline, Long> getPipelinesLoad(
            Map<NioThread, Map<MigratablePipeline, Long>> pipelinesLoadPerOwnerBeforeLoad) {
        return pipelinesLoadPerOwnerBeforeLoad.values().stream()
                .flatMap(v -> v.entrySet().stream())
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<NioThread, Map<MigratablePipeline, Long>> getPipelinesLoadPerOwner(HazelcastInstance hz) {
        ServerConnectionManager cm = getConnectionManager(hz);
        return cm.getConnections().stream()
                .map(conn -> (NioChannel) ((TcpServerConnection) conn).getChannel())
                .flatMap(channel -> Stream.of(channel.inboundPipeline(), channel.outboundPipeline()))
                .collect(Collectors.groupingBy(MigratablePipeline::owner,
                        Collectors.toMap(Function.<MigratablePipeline>identity(), MigratablePipeline::load)));
    }

    private StringBuilder debug(HazelcastInstance hz,
                                Map<NioThread, Map<MigratablePipeline, Long>> pipelinesLoadPerOwnerBeforeLoad) {
        StringBuilder sb = new StringBuilder();
        sb.append("--- Before load:\n");
        sb.append(debug(pipelinesLoadPerOwnerBeforeLoad));
        sb.append("\n");
        sb.append("--- After load:\n");
        sb.append(debug(hz));
        return sb;
    }

    private StringBuilder debug(Map<NioThread, Map<MigratablePipeline, Long>> pipelinesLoadPerOwnerBeforeLoad) {
        StringBuilder sbIn = new StringBuilder();
        sbIn.append("in owners\n");
        StringBuilder sbOut = new StringBuilder();
        sbOut.append("out owners\n");
        for (Map.Entry<NioThread, Map<MigratablePipeline, Long>> entry : pipelinesLoadPerOwnerBeforeLoad.entrySet()) {
            NioThread nioThread = entry.getKey();
            StringBuilder sb = nioThread.getName().contains("thread-in") ? sbIn : sbOut;
            sb.append(entry.getKey()).append("\n");
            for (Map.Entry<MigratablePipeline, Long> pipelineEntry : entry.getValue().entrySet()) {
                MigratablePipeline pipeline = pipelineEntry.getKey();
                sb.append("\t").append(pipeline).append(" load: ").append(pipelineEntry.getValue()).append("\n");
            }
        }
        return sbIn.append(sbOut);
    }

    private StringBuilder debug(HazelcastInstance hz) {
        TcpServer networkingService = (TcpServer) getNode(hz).getServer();
        NioNetworking networking = (NioNetworking) networkingService.getNetworking();
        ServerConnectionManager cm = getNode(hz).getServer().getConnectionManager(EndpointQualifier.MEMBER);

        StringBuilder sb = new StringBuilder();
        sb.append("in owners\n");
        for (NioThread in : networking.getInputThreads()) {
            sb.append(in).append(": ").append(in.getEventCount()).append("\n");

            for (ServerConnection connection : cm.getConnections()) {
                TcpServerConnection tcpConnection = (TcpServerConnection) connection;
                NioInboundPipeline inboundPipeline = ((NioChannel) tcpConnection.getChannel()).inboundPipeline();
                if (inboundPipeline.owner() == in) {
                    sb.append("\t").append(inboundPipeline).append(" load: ").append(inboundPipeline.load()).append("\n");
                }
            }
        }
        sb.append("out owners\n");
        for (NioThread in : networking.getOutputThreads()) {
            sb.append(in).append(": ").append(in.getEventCount()).append("\n");

            for (ServerConnection connection : cm.getConnections()) {
                TcpServerConnection tcpConnection = (TcpServerConnection) connection;
                NioOutboundPipeline outboundPipeline = ((NioChannel) tcpConnection.getChannel()).outboundPipeline();
                if (outboundPipeline.owner() == in) {
                    sb.append("\t").append(outboundPipeline).append(" load:").append(outboundPipeline.load()).append("\n");
                }
            }
        }
        return sb;
    }
}
