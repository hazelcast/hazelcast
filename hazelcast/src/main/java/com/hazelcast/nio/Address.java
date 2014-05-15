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

package com.hazelcast.nio;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

public final class Address implements IdentifiedDataSerializable {

    public static final int ID = 1;

    private static final byte IPV4 = 4;
    private static final byte IPV6 = 6;

    private int port = -1;
    private String host;
    private byte type;

    private String scopeId;
    private boolean hostSet;

    public Address() {
    }

    public Address(String host, int port) throws UnknownHostException {
        this(host, InetAddress.getByName(host), port);
        hostSet = !AddressUtil.isIpAddress(host);
    }

    public Address(InetAddress inetAddress, int port) {
        this(null, inetAddress, port);
        hostSet = false;
    }

    public Address(InetSocketAddress inetSocketAddress) {
        this(inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    }

    private Address(final String hostname, final InetAddress inetAddress, final int port) {
        this.type = (inetAddress instanceof Inet4Address) ? IPV4 : IPV6;
        final String[] addressArgs = inetAddress.getHostAddress().split("\\%");
        this.host = hostname != null ? hostname : addressArgs[0];
        if (addressArgs.length == 2) {
            scopeId = addressArgs[1];

        }
        this.port = port;
    }

    public Address(Address address) {
        this.host = address.host;
        this.port = address.port;
        this.type = address.type;
        this.scopeId = address.scopeId;
        this.hostSet = address.hostSet;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(port);
        out.write(type);
        if (host != null) {
            byte[] address = stringToBytes(host);
            out.writeInt(address.length);
            out.write(address);
        } else {
            out.writeInt(0);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        port = in.readInt();
        type = in.readByte();
        int len = in.readInt();
        if (len > 0) {
            byte[] address = new byte[len];
            in.readFully(address);
            host = bytesToString(address);
        }
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "Address[" + getHost() + "]:" + port;
    }

    public int getPort() {
        return port;
    }

    public InetAddress getInetAddress() throws UnknownHostException {
        return InetAddress.getByName(getScopedHost());
    }

    public InetSocketAddress getInetSocketAddress() throws UnknownHostException {
        return new InetSocketAddress(getInetAddress(), port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Address)) {
            return false;
        }
        final Address address = (Address) o;
        return port == address.port && this.type == address.type && this.host.equals(address.host);
    }

    @Override
    public int hashCode() {
        int result = port;
        result = 31 * result + host.hashCode();
        return result;
    }

    public boolean isIPv4() {
        return type == IPV4;
    }

    public boolean isIPv6() {
        return type == IPV6;
    }

    public String getScopeId() {
        return isIPv6() ? scopeId : null;
    }

    public void setScopeId(final String scopeId) {
        if (isIPv6()) {
            this.scopeId = scopeId;
        }
    }

    public String getScopedHost() {
        return (isIPv4() || hostSet || scopeId == null) ? getHost()
                : getHost() + "%" + scopeId;
    }

    @Override
    public int getFactoryId() {
        return Data.FACTORY_ID;
    }

    @Override
    public int getId() {
        return ID;
    }
}
