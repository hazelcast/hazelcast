package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

public abstract class AbstractOperationService implements InternalOperationService {

    protected final NodeEngineImpl nodeEngine;
    protected final Node node;
    protected final ILogger logger;

    public AbstractOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(getClass().getName());
    }

    @Override
    public final boolean send(final Operation op, final int partitionId, final int replicaIndex) {
        Address target = nodeEngine.getPartitionService().getPartition(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.warning("No target available for partition: " + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, target);
    }

    @Override
    public final boolean send(final Operation op, final Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        } else {
            return send(op, node.getConnectionManager().getOrConnect(target));
        }
    }

    protected final int getPartitionIdForExecution(Operation op) {
        return op instanceof PartitionAwareOperation ? op.getPartitionId() : -1;
    }

    @Override
    public final boolean send(final Operation op, final Connection connection) {
        Data data = nodeEngine.toData(op);
        final int partitionId = getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP);
        if (op instanceof ResponseOperation) {
            packet.setHeader(Packet.HEADER_RESPONSE);
        }
        return nodeEngine.send(packet, connection);
    }

    @Override
    public void start() {
        //nop-op
    }
}
