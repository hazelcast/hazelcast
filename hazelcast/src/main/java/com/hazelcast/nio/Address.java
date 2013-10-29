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

public final class Address implements IdentifiedDataSerializable {

    public static final int ID = 1;

    private static final long serialVersionUID = -7626390274220424603L;
    private static final byte IPv4 = 4;
    private static final byte IPv6 = 6;

    private int port = -1;
    private String host;
    private String site;
    private String rack;
    private String process;
    private byte type;

    private transient String scopeId;
    private transient boolean hostSet;

    public Address() {
    }

    public Address(String site, String rack, String host, String process, int port) throws UnknownHostException {
        this(site, rack, host, InetAddress.getByName(host), process, port) ;
        hostSet = !AddressUtil.isIpAddress(host);
    }


    public Address(String host, int port) throws UnknownHostException {
        this(null, null,
                host,
                InetAddress.getByName(host),
                null,
                port) ;
        hostSet = !AddressUtil.isIpAddress(host);
    }

    public Address(String site, String rack, InetAddress inetAddress, int port) {
        this(site, rack, null, inetAddress, null, port);
        hostSet = false;
    }

    public Address(InetAddress inetAddress, int port) {
        this(null, null, inetAddress, port);
        hostSet = false;
    }


    public Address(String site, String rack, InetSocketAddress inetSocketAddress) {
        this(site, rack, inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    }

    public Address(InetSocketAddress inetSocketAddress) {
        this(null, null,
                System.getProperty("hazelcast.address.rack"),
                inetSocketAddress.getAddress(),
                null,
                inetSocketAddress.getPort());
    }


    private Address(final String site, final String rack, final String hostname, final InetAddress inetAddress, final String process, final int port) {
        this.type = (inetAddress instanceof Inet4Address) ? IPv4 : IPv6;
        final String[] addressArgs = inetAddress.getHostAddress().split("\\%");
        this.host = hostname != null ? hostname : addressArgs[0];
        if (addressArgs.length == 2) {
            scopeId = addressArgs[1];

        }
        this.port = port;
        this.site = site == null ? System.getProperty("hazelcast.address.site") : site;
        this.rack = rack == null ? System.getProperty("hazelcast.address.rack") : rack;
        this.process = (process == null) ? System.getProperty("hazelcast.address.process") : process;
        this.process = this.process == null ? ProcessIDFactory.getUuid() : this.process;
    }

    public Address(Address address) {
        this.site = address.site;
        this.rack = address.rack;
        this.host = address.host;
        this.port = address.port;
        this.type = address.type;
        this.scopeId = address.scopeId;
        this.hostSet = address.hostSet;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(port);
        out.write(type);
        if (site != null) {
            byte[] address = site.getBytes();
            out.writeInt(address.length);
            out.write(address);
        } else {
            out.writeInt(0);
        }
        if (rack != null) {
            byte[] address = rack.getBytes();
            out.writeInt(address.length);
            out.write(address);
        } else {
            out.writeInt(0);
        }
        if (host != null) {
            byte[] address = host.getBytes();
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
            site = new String(address);
        }
        len = in.readInt();
        if (len > 0) {
            byte[] address = new byte[len];
            in.readFully(address);
            rack = new String(address);
        }
        len = in.readInt();
        if (len > 0) {
            byte[] address = new byte[len];
            in.readFully(address);
            host = new String(address);
        }
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "Address[" +
                "site='" + site + '\'' +
                ", rack='" + rack + '\'' +
                ", host='" + host + '\'' +
                ", process='" + process + '\'' +
                ", port=" + port +
                ']';
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Address address = (Address) o;

        if (port != address.port) return false;
        if (type != address.type) return false;
        if (host != null ? !host.equals(address.host) : address.host != null) return false;
        if (rack != null ? !rack.equals(address.rack) : address.rack != null) return false;
        if (site != null ? !site.equals(address.site) : address.site != null) return false;
        return !(process != null ? !process.equals(address.process) : address.process != null);

    }

    @Override
    public int hashCode() {
        int result  = hash(host.getBytes()) * 29 + port;
        result = 29 * result + (site != null ? site.hashCode() : 0);
        result = 29 * result + (rack != null ? rack.hashCode() : 0);
        result = 29 * result + (process != null ? process.hashCode() : 0);
        return result;
    }

    private int hash(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) {
            hash = (hash * 29) + b;
        }
        return hash;
    }

    public boolean isIPv4() {
        return type == IPv4;
    }

    public boolean isIPv6() {
        return type == IPv6;
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

    public String getSite() {
        return site;
    }

    public String getRack() {
        return rack;
    }

    public String getProcess() {
        return process;
    }
}
