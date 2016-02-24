package com.hazelcast.client;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.cluster.impl.JoinMessage;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by bilal on 22/02/16.
 */
public class MulticastDiscoveryStrategy implements DiscoveryStrategy {
    private final Map<String, Comparable> properties;

    public MulticastDiscoveryStrategy(Map<String, Comparable> properties) {
        this.properties = properties;
    }

    @Override
    public void start() {
        System.out.println("test");
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            while (2==2)
            receive();
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>();
        try {
            DiscoveryNode discoveryNode = new SimpleDiscoveryNode(new Address("192.168.57.1", 6701));
            discoveryNodes.add(discoveryNode);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return discoveryNodes;
    }

    @Override
    public void destroy() {

    }

    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private JoinMessage receive() throws Exception {

//        Address bindAddress = new Address("192.168.57.1",5701);
        MulticastConfig multicastConfig = new MulticastConfig();
        MulticastSocket multicastSocket = new MulticastSocket(null);
        try {
            multicastSocket.setReuseAddress(true);
            // bind to receive interface
            multicastSocket.bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
            multicastSocket.setTimeToLive(multicastConfig.getMulticastTimeToLive());
//            if (!bindAddress.getInetAddress().isLoopbackAddress()) {
//                multicastSocket.setInterface(bindAddress.getInetAddress());
//            } else if (multicastConfig.isLoopbackModeEnabled()) {
            multicastSocket.setLoopbackMode(true);
//                multicastSocket.setInterface(bindAddress.getInetAddress());
        } catch (SocketException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        multicastSocket.setReceiveBufferSize(64 * 1024);
        multicastSocket.setSendBufferSize(64 * 1024);
//        multicastSocket.setInterface(bindAddress.getInetAddress());
        multicastSocket.joinGroup(InetAddress.getByName(multicastConfig.getMulticastGroup()));
        
        multicastSocket.setSoTimeout(1000);
        DatagramPacket datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);

        try {
            multicastSocket.receive(datagramPacketReceive);
        } catch (IOException ignore) {
            return null;
        }
        try {
            final byte[] data = datagramPacketReceive.getData();
            final int offset = datagramPacketReceive.getOffset();
            final BufferObjectDataInput input = ((HazelcastClientInstanceImpl) (HazelcastClient.getAllHazelcastClients().iterator().next())).getSerializationService().createObjectDataInput(data);
            input.position(offset);

            final byte packetVersion = input.readByte();
            if (packetVersion != Packet.VERSION) {
//                    logger.warning("Received a JoinRequest with a different packet version! This -> "
//                            + Packet.VERSION + ", Incoming -> " + packetVersion
//                            + ", Sender -> " + datagramPacketReceive.getAddress());
                return null;
            }
            try {
                return input.readObject();
            } finally {
                input.close();
            }
        } catch (Exception e) {
            if (e instanceof EOFException || e instanceof HazelcastSerializationException) {
//                    logger.warning("Received data format is invalid." +
//                            " (An old version of Hazelcast may be running here.)", e);
            } else {
                throw e;
            }
        }
        return null;
    }

}
