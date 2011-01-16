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
import com.hazelcast.util.UnboundedBlockingQueue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class MulticastService implements Runnable {

    private final BlockingQueue<Runnable> queue = new UnboundedBlockingQueue<Runnable>();
    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketSend;
    private final DatagramPacket datagramPacketReceive;
    private final Object sendLock = new Object();
    private final Object receiveLock = new Object();
    final Node node;
    private boolean running = true;
    private List<MulticastListener> lsListeners = new CopyOnWriteArrayList<MulticastListener>();

    public MulticastService(Node node, MulticastSocket multicastSocket) throws Exception {
        this.node = node;
        Config config = node.getConfig();
        this.multicastSocket = multicastSocket;
        int bufferSize = 1024 * 1024;
        this.datagramPacketReceive = new DatagramPacket(new byte[bufferSize], bufferSize);
        this.datagramPacketSend = new DatagramPacket(new byte[bufferSize], bufferSize, InetAddress
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public JoinInfo receive() {
        synchronized (receiveLock) {
            try {
                try {
                    multicastSocket.receive(datagramPacketReceive);
                } catch (SocketTimeoutException ignore) {
                    return null;
                }
                JoinInfo joinInfo = new JoinInfo();
                joinInfo.readFromPacket(datagramPacketReceive);
                return joinInfo;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public void send(JoinInfo joinInfo) {
        synchronized (sendLock) {
            try {
                joinInfo.writeToPacket(datagramPacketSend);
                multicastSocket.send(datagramPacketSend);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
