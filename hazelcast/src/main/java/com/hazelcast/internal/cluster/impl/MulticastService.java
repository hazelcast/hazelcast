/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.cluster.AddressChecker;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.tcp.TcpServerContext;
import com.hazelcast.internal.util.ByteArrayProcessor;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.internal.util.EmptyStatement.ignore;

public final class MulticastService implements Runnable {

    private static final int SEND_OUTPUT_SIZE = 1024;
    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;
    private static final int SOCKET_BUFFER_SIZE = 64 * 1024;
    private static final int SOCKET_TIMEOUT = 1000;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final int JOIN_SERIALIZATION_ERROR_SUPPRESSION_MILLIS = 60000;

    private final List<MulticastListener> listeners = new CopyOnWriteArrayList<>();
    private final Object sendLock = new Object();
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private final ILogger logger;
    private final Node node;
    private final MulticastSocket multicastSocket;
    private final BufferObjectDataOutput sendOutput;
    private final DatagramPacket datagramPacketSend;
    private final DatagramPacket datagramPacketReceive;
    private final AddressChecker joinMessageTrustChecker;

    private final ByteArrayProcessor inputProcessor;
    private final ByteArrayProcessor outputProcessor;

    private long lastLoggedJoinSerializationFailure;
    private volatile boolean running = true;

    private MulticastService(Node node, MulticastSocket multicastSocket)
            throws Exception {
        this.logger = node.getLogger(MulticastService.class.getName());
        this.node = node;
        this.multicastSocket = multicastSocket;

        TcpServerContext context = new TcpServerContext(node, node.nodeEngine);
        this.inputProcessor = node.getNodeExtension().createMulticastInputProcessor(context);
        this.outputProcessor = node.getNodeExtension().createMulticastOutputProcessor(context);

        this.sendOutput = node.getSerializationService().createObjectDataOutput(SEND_OUTPUT_SIZE);

        Config config = node.getConfig();
        MulticastConfig multicastConfig = getActiveMemberNetworkConfig(config).getJoin().getMulticastConfig();
        this.datagramPacketSend = new DatagramPacket(new byte[0], 0, InetAddress.getByName(multicastConfig.getMulticastGroup()),
                multicastConfig.getMulticastPort());
        this.datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);

        Set<String> trustedInterfaces = multicastConfig.getTrustedInterfaces();
        ILogger logger = node.getLogger(AddressCheckerImpl.class);
        joinMessageTrustChecker = new AddressCheckerImpl(trustedInterfaces, logger);
    }

    public static MulticastService createMulticastService(Address bindAddress, Node node, Config config, ILogger logger) {
        JoinConfig join = getActiveMemberNetworkConfig(config).getJoin();
        if (!node.shouldUseMulticastJoiner(join)) {
            return null;
        }
        MulticastConfig multicastConfig = join.getMulticastConfig();
        MulticastService mcService = null;
        try {
            MulticastSocket multicastSocket = new MulticastSocket(null);
            configureMulticastSocket(multicastSocket, bindAddress, node.getProperties(), multicastConfig, logger);
            mcService = new MulticastService(node, multicastSocket);
            mcService.addMulticastListener(new NodeMulticastListener(node));
        } catch (Exception e) {
            logger.severe(e);
            // fail-fast if multicast is explicitly enabled (i.e. autodiscovery is not used)
            if (multicastConfig.isEnabled()) {
                throw new HazelcastException("Starting the MulticastService failed", e);
            }
        }
        return mcService;
    }

    protected static void configureMulticastSocket(MulticastSocket multicastSocket, Address bindAddress,
            HazelcastProperties hzProperties, MulticastConfig multicastConfig, ILogger logger)
            throws SocketException, IOException, UnknownHostException {
        multicastSocket.setReuseAddress(true);
        // bind to receive interface
        multicastSocket.bind(new InetSocketAddress(multicastConfig.getMulticastPort()));
        multicastSocket.setTimeToLive(multicastConfig.getMulticastTimeToLive());
        try {
            boolean loopbackBind = bindAddress.getInetAddress().isLoopbackAddress();
            Boolean loopbackModeEnabled = multicastConfig.getLoopbackModeEnabled();
            if (loopbackModeEnabled != null) {
                // setting loopbackmode is just a hint - and the argument means "disable"!
                // to check the real value value we call getLoopbackMode() (and again - return value means "disabled")
                multicastSocket.setLoopbackMode(!loopbackModeEnabled);
            }
            // If LoopBack mode is not enabled (i.e. getLoopbackMode return true) and bind address is a loopback one,
            // then print a warning
            if (loopbackBind && multicastSocket.getLoopbackMode()) {
                logger.warning("Hazelcast is bound to " + bindAddress.getHost() + " and loop-back mode is "
                        + "disabled. This could cause multicast auto-discovery issues "
                        + "and render it unable to work. Check your network connectivity, try to enable the "
                        + "loopback mode and/or force -Djava.net.preferIPv4Stack=true on your JVM.");
            }

            // warning: before modifying lines below, take a look at these links:
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4417033
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6402758
            // https://github.com/hazelcast/hazelcast/pull/19251#issuecomment-891375270
            boolean callSetInterface = OsHelper.isMac() || !loopbackBind;
            String propSetInterface = hzProperties.getString(ClusterProperty.MULTICAST_SOCKET_SET_INTERFACE);
            if (propSetInterface != null) {
                callSetInterface = Boolean.parseBoolean(propSetInterface);
            }
            if (callSetInterface) {
                multicastSocket.setInterface(bindAddress.getInetAddress());
            }
        } catch (Exception e) {
            logger.warning(e);
        }
        multicastSocket.setReceiveBufferSize(SOCKET_BUFFER_SIZE);
        multicastSocket.setSendBufferSize(SOCKET_BUFFER_SIZE);
        String multicastGroup = hzProperties.getString(ClusterProperty.MULTICAST_GROUP);
        if (multicastGroup == null) {
            multicastGroup = multicastConfig.getMulticastGroup();
        }
        multicastConfig.setMulticastGroup(multicastGroup);
        multicastSocket.joinGroup(InetAddress.getByName(multicastGroup));
        multicastSocket.setSoTimeout(SOCKET_TIMEOUT);
    }

    public void addMulticastListener(MulticastListener multicastListener) {
        listeners.add(multicastListener);
    }

    public void removeMulticastListener(MulticastListener multicastListener) {
        listeners.remove(multicastListener);
    }

    public void stop() {
        try {
            if (!running && multicastSocket.isClosed()) {
                return;
            }
            try {
                multicastSocket.close();
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            running = false;
            if (!stopLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warning("Failed to shutdown MulticastService in " + SHUTDOWN_TIMEOUT_SECONDS + " seconds!");
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
            ignore(ignored);
        }
        stopLatch.countDown();
    }

    @SuppressWarnings("WhileLoopSpinsOnField")
    @Override
    public void run() {
        try {
            while (running) {
                try {
                    final JoinMessage joinMessage = receive();
                    if (joinMessage != null && joinMessageTrustChecker.isTrusted(joinMessage.getAddress())) {
                        for (MulticastListener multicastListener : listeners) {
                            try {
                                multicastListener.onMessage(joinMessage);
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

    private JoinMessage receive() {
        try {
            try {
                multicastSocket.receive(datagramPacketReceive);
            } catch (IOException ignore) {
                return null;
            }
            try {
                final byte[] data = datagramPacketReceive.getData();
                final int offset = datagramPacketReceive.getOffset();
                final int length = datagramPacketReceive.getLength();

                final byte[] processed = inputProcessor != null ? inputProcessor.process(data, offset, length) : data;
                final BufferObjectDataInput input = node.getSerializationService().createObjectDataInput(processed);
                if (inputProcessor == null) {
                    // If pre-processed the offset is already taken into account.
                    input.position(offset);
                }

                final byte packetVersion = input.readByte();
                if (packetVersion != Packet.VERSION) {
                    logger.warning("Received a JoinRequest with a different packet version, or encrypted. "
                            + "Verify that the sender Node, doesn't have symmetric-encryption on. This -> "
                            + Packet.VERSION + ", Incoming -> " + packetVersion
                            + ", Sender -> " + datagramPacketReceive.getAddress());
                    return null;
                }
                return input.readObject();
            } catch (Exception e) {
                if (e instanceof EOFException || e instanceof HazelcastSerializationException) {
                    long now = System.currentTimeMillis();
                    if (now - lastLoggedJoinSerializationFailure > JOIN_SERIALIZATION_ERROR_SUPPRESSION_MILLIS) {
                        lastLoggedJoinSerializationFailure = now;
                        logger.warning("Received a JoinRequest with an incompatible binary-format. "
                                + "An old version of Hazelcast may be using the same multicast discovery port. "
                                + "Are you running multiple Hazelcast clusters on this host? "
                                + "(This message will be suppressed for 60 seconds). ");
                    }
                } else if (e instanceof GeneralSecurityException) {
                    logger.warning("Received a JoinRequest with an incompatible encoding. "
                            + "Symmetric-encryption is enabled on this node, the remote node either doesn't have it on, "
                            + "or it uses different cipher."
                            + "(This message will be suppressed for 60 seconds). ");
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            logger.warning(e);
        }
        return null;
    }

    public void send(JoinMessage joinMessage) {
        if (!running) {
            return;
        }

        final BufferObjectDataOutput out = sendOutput;
        synchronized (sendLock) {
            try {
                out.writeByte(Packet.VERSION);
                out.writeObject(joinMessage);
                byte[] processed = outputProcessor != null ? outputProcessor.process(out.toByteArray()) : out.toByteArray();
                datagramPacketSend.setData(processed);
                multicastSocket.send(datagramPacketSend);
                out.clear();
            } catch (IOException e) {
                // usually catching EPERM errno
                // see https://github.com/hazelcast/hazelcast/issues/7198
                // For details about the causes look at the following discussion:
                // https://groups.google.com/forum/#!msg/comp.protocols.tcp-ip/Qou9Sfgr77E/mVQAPaeI-VUJ
                logger.warning("Sending multicast datagram failed. Exception message saying the operation is not permitted "
                        + "usually means the underlying OS is not able to send packets at a given pace. "
                        + "It can be caused by starting several hazelcast members in parallel when the members send "
                        + "their join message nearly at the same time.", e);
            }
        }
    }
}
