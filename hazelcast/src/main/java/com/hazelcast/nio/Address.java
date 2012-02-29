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

package com.hazelcast.nio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Address implements DataSerializable {

    private static final long serialVersionUID = -7626390274220424603L;

    private byte[] ip;

    private int port = -1;

    public Address() {
        this.ip = new byte[4];
    }

    public Address(InetSocketAddress inetSocketAddress) {
        this.ip = inetSocketAddress.getAddress().getAddress();
        this.port = inetSocketAddress.getPort();
    }

    public Address(InetAddress inetAddress, int port) {
        this(new InetSocketAddress(inetAddress, port));
    }

    public Address(Address address) {
        this.ip = address.copyIP();
        this.port = address.getPort();
    }

    public Address(String address, int port) throws UnknownHostException {
        this.port = port;
        this.ip = InetAddress.getByName(address).getAddress();
    }

    public Address(byte[] ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public static String toString(byte[] ip) {
        return (ip[0] & 0xff) + "." + (ip[1] & 0xff) + "." + (ip[2] & 0xff) + "." + (ip[3] & 0xff);
    }

    public void writeData(DataOutput out) throws IOException {
        out.write(ip);
        out.writeInt(port);
    }

    public void readData(DataInput in) throws IOException {
        in.readFully(ip);
        port = in.readInt();
    }

    public void readObject(ByteBuffer buffer) {
        buffer.get(ip);
        port = buffer.getInt();
    }

    public void writeObject(ByteBuffer buffer) {
        buffer.put(ip);
        buffer.putInt(port);
    }

    public String getHost() {
        return toString(ip);
    }

    @Override
    public String toString() {
        return "Address[" + getHost() + ":" + port + "]";
    }

    public int getPort() {
        return port;
    }

    public InetAddress getInetAddress() throws UnknownHostException {
        return getInetSocketAddress().getAddress();
    }

    public InetSocketAddress getInetSocketAddress() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getByName(getHost()), port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Address)) return false;
        final Address address = (Address) o;
        return port == address.port && Arrays.equals(ip, address.ip);
    }

    @Override
    public int hashCode() {
        return hash(ip) * 29 + port;
    }

    private int hash(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) {
            hash = (hash * 29) + b;
        }
        return hash;
    }

    public byte[] copyIP() {
        byte[] newOne = new byte[ip.length];
        System.arraycopy(ip, 0, newOne, 0, ip.length);
        return newOne;
    }
}
