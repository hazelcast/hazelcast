package com.hazelcast.client.connection;

import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.hazelfast.BlockingChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.nio.IOUtil.closeResource;

public final class ThreadLocalClientBlockingChannelPool
        implements ClientBlockingChannelPool {

    private final static ThreadLocal<Map<Address, ClientBlockingChannel>> threadLocal = ThreadLocal.withInitial(HashMap::new);
    private final ClientPartitionService clientPartitionService;
    private final ILogger logger;
    private final ILogger clientChannelLogger;

    public ThreadLocalClientBlockingChannelPool(ClientPartitionService clientPartitionService,
                                                LoggingService loggingService) {
        this.clientPartitionService = clientPartitionService;
        this.logger = loggingService.getLogger(ThreadLocalClientBlockingChannelPool.class);
        this.clientChannelLogger = loggingService.getLogger(ClientBlockingChannel.class);
    }

    @Override
    public ClientBlockingChannel get(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId can't be negative");
        }

        Map<Address, ClientBlockingChannel> channels = threadLocal.get();
        Address memberAddress = clientPartitionService.getPartitionOwner(partitionId);

        ClientBlockingChannel channel = channels.get(memberAddress);
        if (channel == null) {
            channel = new ClientBlockingChannel(new BlockingChannel.Context()
                    .hostname(memberAddress.getHost())
                    .port(memberAddress.getPort() + 10000)
                    .logger(clientChannelLogger));
            channel.connect();
            channel.attributeMap().put("member", memberAddress);
            channels.put(memberAddress, channel);
        }
        return channel;
    }

    @Override
    public void release(int partitionId, ClientBlockingChannel channel, Throwable cause) {
        if (cause == null) {
            return;
        }

        Map<Address, ClientBlockingChannel> channels = threadLocal.get();
        Address address = (Address) channel.attributeMap().get("member");
        channels.remove(address);
        closeResource(channel);
    }
}
