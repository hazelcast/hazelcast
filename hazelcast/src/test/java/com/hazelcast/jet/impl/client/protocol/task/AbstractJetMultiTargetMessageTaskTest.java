package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.mocknetwork.MockServerConnection;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractJetMultiTargetMessageTaskTest extends JetTestSupport {
    protected static final String OTHER_EXCEPTION_MESSAGE = "null";
    protected static final Map<Member, Object> MEMBER_LEFT_EXCEPTION_RESULT =
            Collections.singletonMap(new SimpleMemberImpl(), new MemberLeftException());
    protected static final Map<Member, Object> OTHER_EXCEPTION_RESULT =
            Collections.singletonMap(new SimpleMemberImpl(), new OtherException(OTHER_EXCEPTION_MESSAGE));

    protected Node node;
    protected Connection connection;

    @Before
    public void init() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        NodeEngineImpl nodeEngine = Accessors.getNodeEngineImpl(hazelcastInstance);
        node = nodeEngine.getNode();
        connection = new MockServerConnection(node.address, node.address, null, null, null, null);
    }

    protected static class OtherException extends Exception {
        public OtherException(String message) {
            super(message);
        }
    }
}