/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.Address;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Holds the socket to one of the members of Hazelcast Cluster.
 *
 * @author fuad-malikov
 */
public class Connection {

    private static final int BUFFER_SIZE = 16 << 10; // 32k
    private final Socket socket;
    private final InetSocketAddress address;
    private final int id;
    private final DataOutputStream dos;
    private final DataInputStream dis;
    boolean headersWritten = false;
    boolean headerRead = false;

    private final int timeout;

    /**
     * Creates the Socket to the given host and port
     *
     * @param host ip address of the host
     * @param port port of the host
     * @throws UnknownHostException
     * @throws IOException
     */
    public Connection(String host, int port, int id) {
        this(3000, new InetSocketAddress(host, port), id);
    }

    public Connection(int timeout, InetSocketAddress address, int id) {
        this.id = id;
        this.address = address;
        this.timeout=timeout;
        try {
            final InetSocketAddress isa = new InetSocketAddress(address.getAddress(), address.getPort());
            final Socket socket = new Socket();
            try {
                socket.setKeepAlive(true);
//                socket.setTcpNoDelay(true);
                socket.setSoLinger(true, 5);
//                socket.setSendBufferSize(BUFFER_SIZE);
//                socket.setReceiveBufferSize(BUFFER_SIZE);
                socket.connect(isa, timeout);
            } catch (IOException e) {
                socket.close();
                throw e;
            }

            this.socket = socket;
            this.dos = new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream(), BUFFER_SIZE));
//            this.dos = new DataOutputStream(socket.getOutputStream());
            this.dis = new DataInputStream(new BufferedInputStream(this.socket.getInputStream(), BUFFER_SIZE));
//            this.dis = new DataInputStream(socket.getInputStream());
        } catch (Exception e) {
            throw new ClusterClientException(e);
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public int getVersion() {
        return id;
    }

    public void close() throws IOException {
        socket.close();
        dos.close();
        dis.close();
    }

    @Override
    public String toString() {
        return "Connection [" + id + "]" + " [" + address + " -> " +
               socket.getInetAddress().getHostAddress() + ":" + socket.getPort() + "]";
    }

    public DataOutputStream getOutputStream() {
        return dos;
    }

    public DataInputStream getInputStream() {
        return dis;
    }

    public Member getMember() {
        return new MemberImpl(new Address(address), false);
    }
}
