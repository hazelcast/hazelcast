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

import com.hazelcast.impl.ClusterManager.JoinRequest;
import com.hazelcast.nio.Address;

import java.io.*;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;

class MulticastService implements Runnable {

    private MulticastSocket multicastSocket;
    private DatagramPacket datagramPacketSend;
    private DatagramPacket datagramPacketReceive;
    private int bufferSize = 1 * 1024;
    private volatile boolean running = true;

    private static final MulticastService instance = new MulticastService();

    private MulticastService() {
    }

    public static MulticastService get() {
        return instance;
    }

    public void init(MulticastSocket multicastSocket) throws Exception {
        Config config = Config.get();
        this.multicastSocket = multicastSocket;
        this.datagramPacketReceive = new DatagramPacket(new byte[bufferSize], bufferSize);
        this.datagramPacketSend = new DatagramPacket(new byte[bufferSize], bufferSize, InetAddress
                .getByName(config.join.multicastConfig.multicastGroup),
                config.join.multicastConfig.multicastPort);
        running = true;

    }

    public void stop() {
        this.running = false;
    }

    public void run() {
        while (running) {
            try {
                final JoinInfo joinInfo = receive();
                if (joinInfo != null) {
                    if (!Node.get().address.equals(joinInfo.address)) {
                        if (Config.get().groupName.equals(joinInfo.groupName)) {
                            if (Config.get().groupPassword.equals(joinInfo.groupPassword)) {
                                if (Node.get().master()) {
                                    if (joinInfo.request) {
                                        send(joinInfo.copy(false, Node.get().address));
                                    }
                                } else {
                                    if (!Node.get().joined() && !joinInfo.request) {
                                        if (Node.get().masterAddress == null) {
                                            Node.get().masterAddress = new Address(joinInfo.address);
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
                    try {
                        JoinInfo joinInfo = new JoinInfo();
                        joinInfo.readFromPacket(datagramPacketReceive);
//                        System.out.println("M.Received " + joinInfo);
                        return joinInfo;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (SocketTimeoutException e) {
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
                        int type) {
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
