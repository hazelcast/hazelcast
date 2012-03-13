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

public final class Address implements DataSerializable {

    private static final long serialVersionUID = -7626390274220424603L;

    private int port = -1;
    private String address;

    public Address() {
    }

    public Address(InetSocketAddress inetSocketAddress) {
        this(inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    }

    public Address(String address, int port) throws UnknownHostException {
        this(InetAddress.getByName(address), port);
    }

    public Address(InetAddress inetAddress, int port) {
        this.port = port;
        this.address = inetAddress.getHostAddress();
    }

    public Address(Address address) {
        this.address = address.address;
        this.port = address.port;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(port);
        out.writeUTF(address);
    }

    public void readData(DataInput in) throws IOException {
        port = in.readInt();
        address = in.readUTF();
    }

    public void readObject(ByteBuffer buffer) {
        port = buffer.getInt();
        int addressLength = buffer.getInt();
        byte[] bytesAddress = new byte[addressLength];
        buffer.get(bytesAddress);
        address = new String(bytesAddress);
    }

    public void writeObject(ByteBuffer buffer) {
        buffer.putInt(port);
        byte[] bytesAddress = address.getBytes();
        buffer.putInt(bytesAddress.length);
        buffer.put(bytesAddress);
    }

    public String getHost() {
        return address;
    }

    @Override
    public String toString() {
        return "Address[" + getHost() + "]:" + port;
    }

    public int getPort() {
        return port;
    }

    public InetAddress getInetAddress() throws UnknownHostException {
        return getInetSocketAddress().getAddress();
    }

    public InetSocketAddress getInetSocketAddress() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getByName(address), port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Address)) return false;
        final Address address = (Address) o;
        return port == address.port && this.address.equals(address.address);
    }

    @Override
    public int hashCode() {
        return hash(address.getBytes()) * 29 + port;
    }

    private int hash(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) {
            hash = (hash * 29) + b;
        }
        return hash;
    }
}
