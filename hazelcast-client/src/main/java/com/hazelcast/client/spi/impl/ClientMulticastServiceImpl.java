package com.hazelcast.client.spi.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.ClientMulticastService;
import com.hazelcast.cluster.MulticastListener;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientMulticastServiceImpl implements ClientMulticastService, Runnable {
    private static final int THREAD_CLOSE_TIMEOUT = 5;
    private static final int DATA_OUTPUT_BUFFER_SIZE = 1024;
    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private final ILogger logger = Logger.getLogger(ClientMulticastService.class);
    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketSend;
    private final DatagramPacket datagramPacketReceive;
    private final Object sendLock = new Object();
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final List<MulticastListener> listeners = new CopyOnWriteArrayList<MulticastListener>();
    private final HazelcastClient client;

    private final BufferObjectDataOutput sendOutput;

    private volatile boolean running = true;

    public ClientMulticastServiceImpl(HazelcastClient client, MulticastSocket multicastSocket) throws Exception {
        this.client = client;
        ClientConfig config = client.getClientConfig();
        this.multicastSocket = multicastSocket;

        sendOutput = client.getSerializationService().createObjectDataOutput(DATA_OUTPUT_BUFFER_SIZE);
        datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);
        final MulticastConfig multicastConfig = config.getNetworkConfig().getDiscoveryConfig();
        datagramPacketSend = new DatagramPacket(new byte[0], 0, InetAddress
                .getByName(multicastConfig.getMulticastGroup()), multicastConfig.getMulticastPort());
        running = true;
    }

    public void addMulticastListener(MulticastListener multicastListener) {
        listeners.add(multicastListener);
    }

    public void removeMulticastListener(MulticastListener multicastListener) {
        listeners.remove(multicastListener);
    }

    public void start() {
        if (client.getClientConfig().getNetworkConfig().getDiscoveryConfig().isEnabled()) {
            final Thread multicastServiceThread = new Thread(client.getThreadGroup(), this,
                    client.getName() + ".multicast-discovery");
            multicastServiceThread.start();
        }
    }

    public void stop() {
        try {
            if (!running && multicastSocket.isClosed()) {
                return;
            }
            try {
                multicastSocket.close();
            } catch (Throwable ignored) {
                logger.finest("exception while closing multicast socket");
            }
            running = false;
            if (!stopLatch.await(THREAD_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                logger.warning("Failed to shutdown MulticastService in 5 seconds!");
            }
        } catch (Throwable e) {
            logger.warning(e);
        }
    }

    private void cleanup() {
        running = false;
        try {
            sendOutput.close();
            datagramPacketReceive.setData(new byte[0]);
            datagramPacketSend.setData(new byte[0]);
        } catch (Throwable ignored) {
            logger.finest("exception while closing client multicast service");
        }
        stopLatch.countDown();
    }

    @SuppressWarnings("WhileLoopSpinsOnField")
    @Override
    public void run() {
        try {
            while (running) {
                try {
                    final Object message = receive();
                    if (message != null) {
                        for (MulticastListener multicastListener : listeners) {
                            try {
                                multicastListener.onMessage(message);
                            } catch (Exception e) {
                                logger.warning(e);
                            }
                        }
                    }
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
        } finally {
            cleanup();
        }
    }

    private Object receive() {
        try {
            try {
                multicastSocket.receive(datagramPacketReceive);
            } catch (IOException ignore) {
                return null;
            }
            try {
                final byte[] data = datagramPacketReceive.getData();
                final int offset = datagramPacketReceive.getOffset();
                final BufferObjectDataInput input = client.getSerializationService().createObjectDataInput(data);
                input.position(offset);

                final byte packetVersion = input.readByte();
                if (packetVersion != Packet.VERSION) {
                    logger.warning("Received a multicast message with a different packet version! This -> "
                            + Packet.VERSION + ", Incoming -> " + packetVersion
                            + ", Sender -> " + datagramPacketReceive.getAddress());
                    return null;
                }
                try {
                    return input.readObject();
                } finally {
                    input.close();
                }
            } catch (Exception e) {
                if (e instanceof EOFException || e instanceof HazelcastSerializationException) {
                    logger.warning("Received data format is invalid."
                            + " (An old version of Hazelcast may be running here.)", e);
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            logger.warning(e);
        }
        return null;
    }

    public void send(Object message) {
        if (!running) {
            return;
        }
        final BufferObjectDataOutput out = sendOutput;
        synchronized (sendLock) {
            try {
                out.writeByte(Packet.VERSION);
                out.writeObject(message);
                datagramPacketSend.setData(out.toByteArray());
                multicastSocket.send(datagramPacketSend);
                out.clear();
            } catch (IOException e) {
                logger.warning("You probably have too long Hazelcast configuration!", e);
            }
        }
    }
}
