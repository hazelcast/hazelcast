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
import com.hazelcast.nio.Packet;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

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
/*
    private MulticastSocket createMulticastSocket(MulticastConfig multicastConfig)
    {
        MulticastSocket multicastSocket = null;
        try {
            if (multicastConfig.isEnabled()) {
                multicastSocket = new MulticastSocket(null);
                multicastSocket.setReuseAddress(true);
                // bind to receive interface
                multicastSocket.bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
                multicastSocket.setTimeToLive(multicastConfig.getMulticastTimeToLive());
                try {
                    // set the send interface
                    final Address bindAddress = addressPicker.getBindAddress();
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4417033
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6402758
                    if (!bindAddress.getInetAddress().isLoopbackAddress()) {
                        multicastSocket.setInterface(bindAddress.getInetAddress());
                    }
                } catch (Exception e) {
                    logger.warning(e);
                }
                multicastSocket.setReceiveBufferSize(64 * 1024);
                multicastSocket.setSendBufferSize(64 * 1024);
                String multicastGroup = System.getProperty("hazelcast.multicast.group");
                if (multicastGroup == null) {
                    multicastGroup = multicastConfig.getMulticastGroup();
                }
                multicastConfig.setMulticastGroup(multicastGroup);
                multicastSocket.joinGroup(InetAddress.getByName(multicastGroup));
                multicastSocket.setSoTimeout(1000);
                mcService = new MulticastDiscoveryService(this, multicastSocket);
                //mcService.addMulticastListener(new NodeMulticastDiscoveryListener(this));
            }
        } catch (Exception e) {
            logger.severe(e);
        }

        return mcService;
    }
*/
    public Set<String> getAddresses() {
        final BlockingQueue<DiscoveryMessage> q = new LinkedBlockingQueue<DiscoveryMessage>();
        MulticastListener listener = new MulticastListener() {
            public void onMessage(Object msg) {
                LOGGER.log(Level.FINE, "MulticastListener onMessage " + msg);
                if (msg != null && msg instanceof DiscoveryMessage) {
                    DiscoveryMessage discoveryMessage = (DiscoveryMessage) msg;
                    if (discoveryMessage.getAddress() != null && discoveryMessage.getMemberCount() > 0) {
                        q.add(discoveryMessage);
                    }
                }
            }
        };

        Set<String> addresses = new HashSet<String>();
        ClientMulticastService multicastService = client.getClientMulticastService();
        if(multicastService != null) {
            multicastService.addMulticastListener(listener);
            multicastService.send(createDiscoveryRequest());
            LOGGER.finest("Sent multicast join request");
            try {
                DiscoveryMessage discoveryInfo = q.poll(3, TimeUnit.SECONDS);
                if (discoveryInfo != null) {
                    addresses.add(discoveryInfo.getAddress().toString());
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

    public DiscoveryMessage createDiscoveryRequest() {
        return new DiscoveryMessage(Packet.VERSION, buildInfo.getBuildNumber(), null,
                createClientConfigCheck(), 0);
    }

    private ClientConfigCheck createClientConfigCheck() {
        ClientConfigCheck configCheck = new ClientConfigCheck();
        final GroupConfig groupConfig = config.getGroupConfig();

        configCheck.setGroupName(groupConfig.getName()).setGroupPassword(groupConfig.getPassword());
        return configCheck;
    }

}
