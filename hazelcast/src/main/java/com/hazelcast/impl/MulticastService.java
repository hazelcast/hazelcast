/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.JoinInfo;
import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.PipedZipBufferFactory;
import com.hazelcast.nio.PipedZipBufferFactory.DeflatingPipedBuffer;
import com.hazelcast.nio.PipedZipBufferFactory.InflatingPipedBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

public class MulticastService implements Runnable {

	private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;
	
    private final ILogger logger;
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketSend;
    private final DatagramPacket datagramPacketReceive;
    private final Object sendLock = new Object();
    private final Object receiveLock = new Object();
    final Node node;
    private boolean running = true;
    private List<MulticastListener> lsListeners = new CopyOnWriteArrayList<MulticastListener>();
    
    private final InflatingPipedBuffer inflatingBuffer = PipedZipBufferFactory.createInflatingBuffer(DATAGRAM_BUFFER_SIZE);
    private final DeflatingPipedBuffer deflatingBuffer = PipedZipBufferFactory.createDeflatingBuffer(DATAGRAM_BUFFER_SIZE, Deflater.BEST_SPEED);
    
    public MulticastService(Node node, MulticastSocket multicastSocket) throws Exception {
        this.node = node;
        logger = node.getLogger(MulticastService.class.getName());
        Config config = node.getConfig();
        this.multicastSocket = multicastSocket;
        this.datagramPacketReceive = new DatagramPacket(inflatingBuffer.getInputBuffer().array(), DATAGRAM_BUFFER_SIZE);
        this.datagramPacketSend = new DatagramPacket(deflatingBuffer.getOutputBuffer().array(), DATAGRAM_BUFFER_SIZE, InetAddress
                .getByName(config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastGroup()),
                config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastPort());
        running = true;
    }

    public void addMulticastListener(MulticastListener multicastListener) {
        lsListeners.add(multicastListener);
    }

    public void removeMulticastListener(MulticastListener multicastListener) {
        lsListeners.remove(multicastListener);
    }

    public void stop() {
        try {
            final CountDownLatch l = new CountDownLatch(1);
            queue.put(new Runnable() {
                public void run() {
                    running = false;
                    l.countDown();
                }
            });
            l.await();
        } catch (InterruptedException ignored) {
        }
    }

    public void run() {
        while (running) {
            try {
                Runnable runnable = queue.poll();
                if (runnable != null) {
                    runnable.run();
                    return;
                }
                final JoinInfo joinInfo = receive();
                if (joinInfo != null) {
                    for (MulticastListener multicastListener : lsListeners) {
                        multicastListener.onMessage(joinInfo);
                    }
                }
            } catch (OutOfMemoryError e) {
                node.onOutOfMemory(e);
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public JoinInfo receive() {
        synchronized (receiveLock) {
            try {
            	inflatingBuffer.reset();
                try {
                    multicastSocket.receive(datagramPacketReceive);
                } catch (SocketTimeoutException ignore) {
                    return null;
                }
                try {
                	inflatingBuffer.inflate(datagramPacketReceive.getLength());
                	JoinInfo joinInfo = new JoinInfo();
                	joinInfo.readData(inflatingBuffer.getDataInput());
                	return joinInfo;
				} catch (Exception e) {
					if(e instanceof EOFException || e instanceof DataFormatException) {
						logger.log(Level.FINEST, "Received data format is invalid." +
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
    }

    public void send(JoinInfo joinInfo) {
        synchronized (sendLock) {
            try {
            	deflatingBuffer.reset();
            	joinInfo.writeData(deflatingBuffer.getDataOutput());
                final int count = deflatingBuffer.deflate();
                datagramPacketSend.setData(deflatingBuffer.getOutputBuffer().array(), 0, count);
                multicastSocket.send(datagramPacketSend);
            } catch (IOException e) {
                logger.log(Level.WARNING, "You probably have too long Hazelcast configuration!", e);
            }
        }
    }
}
