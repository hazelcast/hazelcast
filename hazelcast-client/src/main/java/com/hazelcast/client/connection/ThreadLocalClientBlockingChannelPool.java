package com.hazelcast.client.connection;

import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.hazelfast.BlockingChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionService;

import java.util.HashMap;
import java.util.Map;

public final class ThreadLocalClientBlockingChannelPool
        implements ClientBlockingChannelPool {

    private final static ThreadLocal<Map<Address, ClientBlockingChannel>> threadLocal = ThreadLocal.withInitial(HashMap::new);
    private final ClientPartitionService clientPartitionService;
    private final ILogger logger;
    private final ILogger clientChannelLogger;

    public ThreadLocalClientBlockingChannelPool(ClientPartitionService clientPartitionService,
                                                LoggingService loggingService,
                                                PartitionService partitionService) {
        this.clientPartitionService = clientPartitionService;
        this.logger = loggingService.getLogger(ThreadLocalClientBlockingChannelPool.class);
        this.clientChannelLogger = loggingService.getLogger(ClientBlockingChannel.class);
        //partitionService.addMigrationListener(new MigrationListenerImpl());
        partitionService.addPartitionLostListener(new PartitionLostListenerImpl());
     }

    @Override
    public ClientBlockingChannel get(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId can't be negative");
        }

        Map<Address, ClientBlockingChannel> channels = threadLocal.get();
        //todo: this call causes litter
        Address memberAddress = clientPartitionService.getPartitionOwner(partitionId);

        ClientBlockingChannel channel = channels.get(memberAddress);
        if (channel == null) {
            channel = new ClientBlockingChannel(new BlockingChannel.Context()
                    .hostname(memberAddress.getHost())
                    .port(memberAddress.getPort() + 10000)
                    .logger(clientChannelLogger));
            channel.connect();
            channels.put(memberAddress, channel);
        }
        return channel;
    }

    @Override
    public void release(ClientBlockingChannel channel) {
        //no-op
    }

    private static class MigrationListenerImpl implements MigrationListener {
        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {
        }
    }

    private static class PartitionLostListenerImpl implements PartitionLostListener {
        @Override
        public void partitionLost(PartitionLostEvent event) {
        }
    }
}
