/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterClockImpl;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.ClusterStateManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.instance.impl.TestUtil.toData;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class JoinShutdownTest {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private HazelcastInstance hz;

    @Parameter
    public boolean joinedBefore;

    @Parameters(name = "joinedBefore:{0}")
    public static Object[] data() {
        return new Object[]{false, true};
    }

    @Before
    public void setUp() throws Exception {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        hz = factory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenFinalizeJoinOp() throws Exception {
        assertTrueEventually(() -> getNode(hz).getClusterService().isJoined());

        Node node = getNode(hz);
        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterClockImpl clusterClock = clusterService.getClusterClock();
        ClusterStateManager clusterStateManager = clusterService.getClusterStateManager();
        MemberImpl member = clusterService.getLocalMember();
        Operation op = new FinalizeJoinOp(
                member.getUuid(),
                clusterService.getMembershipManager().getMembersView(),
                new OnJoinOp(Collections.emptyList()),
                new OnJoinOp(Collections.emptyList()),
                clusterClock.getClusterTime(),
                clusterService.getClusterId(),
                clusterClock.getClusterStartTime(),
                clusterStateManager.getState(),
                clusterService.getClusterVersion(),
                node.getPartitionService().createPartitionState(),
                false,
                node.getClusterTopologyIntent()
        );
        OperationAccessor.setCallId(op, 1);

        ByteBuffer buffer = toBuffer(op);
        int position = buffer.position();
        upcast(buffer).position(286);
        buffer.putInt(42_000_000);
        upcast(buffer).position(position);
        upcast(buffer).flip();

        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(buffer, node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenConfigMismatchOp() throws Exception {
        Node node = getNode(hz);
        assertTrueEventually(() -> node.getClusterService().isJoined());
        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(new ConfigMismatchOp("Random reason"), node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenBeforeJoinCheckFailureOp() throws Exception {
        Node node = getNode(hz);
        assertTrueEventually(() -> node.getClusterService().isJoined());
        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(new BeforeJoinCheckFailureOp("Random reason"), node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenAuthenticationFailureOp() throws Exception {
        Node node = getNode(hz);
        assertTrueEventually(() -> node.getClusterService().isJoined());
        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(new AuthenticationFailureOp(), node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    private static void send(Operation operation, InetSocketAddress address) throws Exception {
        send((ByteBuffer) upcast(toBuffer(operation)).flip(), address);
    }

    private static void send(ByteBuffer buffer, InetSocketAddress address) throws Exception {
        // send bytebuffer
        try (SocketChannel socketChannel = SocketChannel.open()) {
            socketChannel.connect(address);
            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        }

        // sleep for member to process the operation
        Thread.sleep(100);
    }

    private static ByteBuffer toBuffer(Operation operation) {
        // op -> packet
        Data data = toData(operation);
        Packet packet = new Packet(data.toByteArray());
        packet.setPacketType(Packet.Type.OPERATION);

        //packet -> bytebuffer
        ByteBuffer buffer = ByteBuffer.allocate(packet.getFrameLength() + 3);
        buffer.put("HZC".getBytes());
        PacketIOHelper packetIOHelper = new PacketIOHelper();
        packetIOHelper.writeTo(packet, buffer);
        return buffer;
    }
}
