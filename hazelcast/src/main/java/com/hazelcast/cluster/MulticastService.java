/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.Packet;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class MulticastService implements Runnable {

    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private final ILogger logger;
    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketSend;
    private final DatagramPacket datagramPacketReceive;
    private final Object sendLock = new Object();
    private volatile boolean running = true;
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private List<MulticastListener> listeners = new CopyOnWriteArrayList<MulticastListener>();
    private final Node node;

    private final Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    private final Inflater inflater = new Inflater();

    private final BufferObjectDataOutput sendOutput;
    private final BufferObjectDataOutput receiveOutput;

    public MulticastService(Node node, MulticastSocket multicastSocket) throws Exception {
        this.node = node;
        logger = node.getLogger(MulticastService.class.getName());
        Config config = node.getConfig();
        this.multicastSocket = multicastSocket;

        sendOutput = node.serializationService.createObjectDataOutput(DATAGRAM_BUFFER_SIZE);
        receiveOutput = node.serializationService.createObjectDataOutput(DATAGRAM_BUFFER_SIZE);

        this.datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);
        final MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        this.datagramPacketSend = new DatagramPacket(new byte[0], 0, InetAddress
                .getByName(multicastConfig.getMulticastGroup()), multicastConfig.getMulticastPort());
        running = true;
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
            }
            running = false;
            if (!stopLatch.await(5, TimeUnit.SECONDS)) {
                logger.log(Level.WARNING, "Failed to shutdown MulticastService in 5 seconds!");
            }
        } catch (Throwable e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    private void cleanup() {
        running = false;
        try {
            sendOutput.close();
            receiveOutput.close();
            datagramPacketReceive.setData(new byte[0]);
            datagramPacketSend.setData(new byte[0]);
        } catch (Throwable ignored) {
        }
        stopLatch.countDown();
    }

    @SuppressWarnings("WhileLoopSpinsOnField")
    public void run() {
        try {
            while (running) {
                try {
                    final JoinMessage joinMessage = receive();
                    if (joinMessage != null) {
                        for (MulticastListener multicastListener : listeners) {
                            try {
                                multicastListener.onMessage(joinMessage);
                            } catch (Exception e) {
                                logger.log(Level.WARNING, e.getMessage(), e);
                            }
                        }
                    }
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                } catch (Exception e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
            }
        } finally {
            cleanup();
        }
    }

    private JoinMessage receive() {
        final BufferObjectDataOutput out = receiveOutput;
        try {
            out.reset();
            inflater.reset();
            try {
                multicastSocket.receive(datagramPacketReceive);
            } catch (IOException ignore) {
                return null;
            }
            try {
                inflater.setInput(datagramPacketReceive.getData(),
                        datagramPacketReceive.getOffset(), datagramPacketReceive.getLength());
                final int count = inflater.inflate(out.getBuffer());
                out.position(count);

                ObjectDataInput input = node.serializationService.createObjectDataInput(out.toByteArray());
                final byte packetVersion = input.readByte();
                if (packetVersion != Packet.PACKET_VERSION) {
                    logger.log(Level.WARNING, "Received a JoinRequest with different packet version: "
                            + packetVersion);
                    return null;
                }
                return input.readObject();
            } catch (Exception e) {
                if (e instanceof EOFException || e instanceof DataFormatException) {
                    logger.log(Level.WARNING, "Received data format is invalid." +
                            " (An old version of Hazelcast may be running here.)", e);
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        return null;
    }

    public void send(JoinMessage joinMessage) {
        if (!running) return;
        final BufferObjectDataOutput out = sendOutput;
        synchronized (sendLock) {
            try {
                out.reset();
                deflater.reset();
                out.writeByte(Packet.PACKET_VERSION);
                out.writeObject(joinMessage);
                deflater.setInput(out.toByteArray());
                out.reset();
                deflater.finish();
                final int count = deflater.deflate(out.getBuffer());
                out.position(count);
                datagramPacketSend.setData(out.toByteArray());
                multicastSocket.send(datagramPacketSend);
            } catch (IOException e) {
                logger.log(Level.WARNING, "You probably have too long Hazelcast configuration!", e);
            }
        }
    }
}
