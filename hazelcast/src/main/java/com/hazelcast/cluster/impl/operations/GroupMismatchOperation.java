package com.hazelcast.cluster.impl.operations;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class GroupMismatchOperation extends AbstractClusterOperation
        implements JoinOperation {

    public GroupMismatchOperation() {
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ConnectionManager connectionManager = nodeEngine.getNode().getConnectionManager();
        Connection connection = getConnection();
        connectionManager.destroyConnection(connection);

        ILogger logger = nodeEngine.getLogger("com.hazelcast.cluster");
        logger.warning("Node could not join cluster at node: " + connection.getEndPoint()
                + " Cause: the target cluster has a different group-name");

        Node node = nodeEngine.getNode();
        node.getJoiner().blacklist(getCallerAddress(), true);
    }
}

