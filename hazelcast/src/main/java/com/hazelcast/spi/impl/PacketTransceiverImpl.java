package com.hazelcast.spi.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.wan.WanReplicationService;

import java.util.concurrent.TimeUnit;


/**
 * Default {@link com.hazelcast.spi.impl.PacketTransceiver} implementation.
 */
public class PacketTransceiverImpl implements PacketTransceiver {

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    private final Node node;
    private final ExecutionService executionService;
    private final ILogger logger;
    private final EventServiceImpl eventService;
    private final WanReplicationService wanReplicationService;
    private final OperationExecutor operationExecutor;

    public PacketTransceiverImpl(Node node,
                                 ILogger logger,
                                 InternalOperationService operationService,
                                 EventServiceImpl eventService,
                                 WanReplicationService wanReplicationService,
                                 ExecutionService executionService) {
        this.node = node;
        this.executionService = executionService;
        this.operationExecutor = operationService.getOperationExecutor();
        this.eventService = eventService;
        this.wanReplicationService = wanReplicationService;
        this.logger = logger;
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
        if (connection == null || !connection.isAlive()) {
            return false;
        }
        final MemberImpl memberImpl = node.getClusterService().getMember(connection.getEndPoint());
        if (memberImpl != null) {
            memberImpl.didWrite();
        }
        return connection.write(packet);
    }

    @Override
    public void receive(Packet packet) {
        if (packet.isHeaderSet(Packet.HEADER_OP)) {
            operationExecutor.execute(packet);
        } else if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
            eventService.handleEvent(packet);
        } else if (packet.isHeaderSet(Packet.HEADER_WAN_REPLICATION)) {
            wanReplicationService.handleEvent(packet);
        } else {
            logger.severe("Unknown packet type! Header: " + packet.getHeader());
        }
    }

    /**
     * Retries sending packet maximum 5 times until connection to target becomes available.
     */
    @Override
    public boolean transmit(Packet packet, Address target) {
        return send(packet, target, null);
    }

    private boolean send(Packet packet, Address target, SendTask sendTask) {
        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getConnection(target);
        if (connection != null) {
            return transmit(packet, connection);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target);
        }

        final int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && node.isActive()) {
            connectionManager.getOrConnect(target, true);
            // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
            executionService.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private final class SendTask implements Runnable {
        private final Packet packet;
        private final Address target;
        private volatile int retries;

        private SendTask(Packet packet, Address target) {
            this.packet = packet;
            this.target = target;
        }

        //retries is incremented by a single thread, but will be read by multiple. So there is no problem.
        @edu.umd.cs.findbugs.annotations.SuppressWarnings("VO_VOLATILE_INCREMENT")
        @Override
        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this);
        }
    }
}
