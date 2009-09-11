/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.JoinRequest;
import com.hazelcast.config.Config;
import com.hazelcast.nio.Address;

import java.io.*;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class MulticastService implements Runnable {

    private MulticastSocket multicastSocket;
    private DatagramPacket datagramPacketSend;
    private DatagramPacket datagramPacketReceive;
    private int bufferSize = 1024;
    private volatile boolean running = true;
    final Node node;
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();

    public MulticastService(Node node, MulticastSocket multicastSocket) throws Exception {
        this.node = node;
        Config config = node.getConfig();
        this.multicastSocket = multicastSocket;
        this.datagramPacketReceive = new DatagramPacket(new byte[bufferSize], bufferSize);
        this.datagramPacketSend = new DatagramPacket(new byte[bufferSize], bufferSize, InetAddress
                .getByName(config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastGroup()),
                config.getNetworkConfig().getJoin().getMulticastConfig().getMulticastPort());
        running = true;

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
        Config config = node.getConfig();
        while (running) {
            try {
                Runnable runnable = queue.poll();
                if (runnable != null) {
                    runnable.run();
                    return;
                }
                final JoinInfo joinInfo = receive();
                if (joinInfo != null) {
                    if (node.address != null && !node.address.equals(joinInfo.address)) {
                        if (config.getGroupName().equals(joinInfo.groupName)) {
                            if (config.getGroupPassword().equals(joinInfo.groupPassword)) {
                                if (node.master()) {
                                    if (joinInfo.request) {
                                        send(joinInfo.copy(false, node.address));
                                    }
                                } else {
                                    if (!node.joined() && !joinInfo.request) {
                                        if (node.masterAddress == null) {
                                            node.masterAddress = new Address(joinInfo.address);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public JoinInfo receive() {
        synchronized (datagramPacketReceive) {
            try {
                try {
                    multicastSocket.receive(datagramPacketReceive);
                    JoinInfo joinInfo = new JoinInfo();
                    joinInfo.readFromPacket(datagramPacketReceive);
                    return joinInfo;
                } catch (SocketTimeoutException ignore) {
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public void send(JoinInfo joinInfo) {
        synchronized (datagramPacketSend) {
            joinInfo.writeToPacket(datagramPacketSend);
            try {
                multicastSocket.send(datagramPacketSend);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static class JoinInfo extends JoinRequest {
        boolean request = true;

        public JoinInfo() {
        }

        public JoinInfo(boolean request, Address address, String groupName, String groupPassword,
                        Node.NodeType type) {
            super(address, groupName, groupPassword, type);
            this.request = request;
        }

        public JoinInfo copy(boolean newRequest, Address newAddress) {
            return new JoinInfo(newRequest, newAddress, groupName, groupPassword, nodeType);
        }

        void writeToPacket(DatagramPacket packet) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            try {
                dos.writeBoolean(request);
                address.writeData(dos);
                dos.writeUTF(groupName);
                dos.writeUTF(groupPassword);
                packet.setData(bos.toByteArray());
                packet.setLength(bos.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        void readFromPacket(DatagramPacket packet) {
            ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData(), 0, packet
                    .getLength());
            DataInputStream dis = new DataInputStream(bis);
            try {
                request = dis.readBoolean();
                address = new Address();
                address.readData(dis);
                groupName = dis.readUTF();
                groupPassword = dis.readUTF();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        @Override
        public String toString() {
            return "JoinInfo{" +
                    "request=" + request + "  " + super.toString() +
                    '}';
        }
    }

}
