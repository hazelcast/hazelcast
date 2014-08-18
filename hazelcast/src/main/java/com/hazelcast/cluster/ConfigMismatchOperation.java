package com.hazelcast.cluster;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

/**
 * When a node wants to join the cluster, its sends its ConfigCheck to the cluster where it is validated.
 * If the ConfigCheck fails, this operation is send to that node to trigger him to shutdown himself. This
 * way he will not join the cluster.
 *
 * @see com.hazelcast.cluster.AuthenticationFailureOperation
 */
public class ConfigMismatchOperation extends AbstractClusterOperation
        implements JoinOperation {

    private String msg;

    public ConfigMismatchOperation() {
    }

    public ConfigMismatchOperation(String msg) {
        this.msg = msg;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(msg);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        msg = in.readUTF();
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();
        ILogger logger = nodeEngine.getLogger("com.hazelcast.cluster");
        logger.severe("Node could not join cluster. A Configuration mismatch was detected: "
                + msg + " Node is going to shutdown now!");
        node.shutdown(true);
    }
}

