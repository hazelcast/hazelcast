package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientMulticastService;
import com.hazelcast.cluster.ClientConfigCheck;
import com.hazelcast.cluster.DiscoveryMessage;
import com.hazelcast.cluster.MulticastListener;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DiscoveryClient {
    private static final ILogger LOGGER = Logger.getLogger(DiscoveryAddressProvider.class);
    private HazelcastClient client;
    private ClientConfig config;
    private final BuildInfo buildInfo;

    public DiscoveryClient(HazelcastClient client, ClientConfig config) {
        this.client = client;
        this.config = config;
        buildInfo = BuildInfoProvider.getBuildInfo();
    }

    public Set<InetSocketAddress> getAddresses() {
        final BlockingQueue<DiscoveryMessage> q = new LinkedBlockingQueue<DiscoveryMessage>();
        MulticastListener listener = new MulticastListener() {
            public void onMessage(Object msg) {
                LOGGER.finest("MulticastListener onMessage " + msg);
                if (msg != null && msg instanceof DiscoveryMessage) {
                    DiscoveryMessage discoveryMessage = (DiscoveryMessage) msg;
                    if(isValidDiscoveryMessage(discoveryMessage)) {
                        q.add(discoveryMessage);
                    }
                }
            }
        };

        int pollTimeout = config.getNetworkConfig().getDiscoveryConfig().getMulticastTimeoutSeconds();
        Set<InetSocketAddress> addresses = new HashSet<InetSocketAddress>();
        ClientMulticastService multicastService = client.getClientMulticastService();
        if(multicastService != null) {
            multicastService.addMulticastListener(listener);
            multicastService.send(createDiscoveryRequest());
            LOGGER.finest("Sent multicast join request");
            try {
                DiscoveryMessage discoveryInfo = q.poll(pollTimeout, TimeUnit.SECONDS);
                if (discoveryInfo != null) {
                    addresses.add(discoveryInfo.getAddress().getInetSocketAddress());
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                LOGGER.warning(e);
            } finally {
                multicastService.removeMulticastListener(listener);
            }
        }
        return addresses;
    }

    private boolean isValidDiscoveryMessage(DiscoveryMessage discoveryMessage) {
        if(discoveryMessage.getMemberCount() <= 0) {
            return false;
        }

        if(!createClientConfigCheck().isCompatible(discoveryMessage.getConfigCheck())) {
            return false;
        }

        return true;
    }

    public DiscoveryMessage createDiscoveryRequest() {
        return new DiscoveryMessage(Packet.VERSION, buildInfo.getBuildNumber(), new Address(),
                createClientConfigCheck(), 0);
    }

    private ClientConfigCheck createClientConfigCheck() {
        ClientConfigCheck configCheck = new ClientConfigCheck();
        final GroupConfig groupConfig = config.getGroupConfig();

        configCheck.setGroupName(groupConfig.getName()).setGroupPassword(groupConfig.getPassword());
        return configCheck;
    }
}
