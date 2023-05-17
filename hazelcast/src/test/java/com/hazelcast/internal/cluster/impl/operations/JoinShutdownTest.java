package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
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
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
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
import static org.jgroups.util.Util.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
// Quick test -> NightlyTest before merge.
// We don't need this test in PR builder IMO.
@Category(QuickTest.class)
public class JoinShutdownTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Parameter
    public boolean joinedBefore;

    @Parameters(name = "joinedBefore:{0}")
    public static Object[] data() {
        return new Object[]{false, true};
    }

    @Before
    public void setUp() throws Exception {
        hz = Hazelcast.newHazelcastInstance();
    }

    @After
    public void tearDown() throws Exception {
        hz.shutdown();
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
        buffer.position(286);
        buffer.putInt(42_000_000);
        buffer.position(position);
        buffer.flip();

        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(buffer);
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenConfigMismatchOp() throws Exception {
        assertTrueEventually(() -> getNode(hz).getClusterService().isJoined());
        if (joinedBefore) {
            getNode(hz).getClusterService().resetJoinState();
        }
        send(new ConfigMismatchOp("Random reason"));
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenBeforeJoinCheckFailureOp() throws Exception {
        assertTrueEventually(() -> getNode(hz).getClusterService().isJoined());
        if (joinedBefore) {
            getNode(hz).getClusterService().resetJoinState();
        }
        send(new BeforeJoinCheckFailureOp("Random reason"));
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenAuthenticationFailureOp() throws Exception {
        assertTrueEventually(() -> getNode(hz).getClusterService().isJoined());
        if (joinedBefore) {
            getNode(hz).getClusterService().resetJoinState();
        }
        send(new AuthenticationFailureOp());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    private static void send(Operation operation) throws Exception {
        send((ByteBuffer) toBuffer(operation).flip());
    }

    private static void send(ByteBuffer buffer) throws Exception {
        // send bytebuffer
        try (SocketChannel socketChannel = SocketChannel.open()) {
            socketChannel.connect(new InetSocketAddress("127.0.0.1", 5701));
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

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }
}
