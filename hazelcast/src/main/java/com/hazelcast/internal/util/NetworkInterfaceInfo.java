/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NetworkInterfaceInfo {
    private final boolean up;
    private final boolean virtual;
    private final boolean loopback;
    private final String name;
    private final List<InetAddress> inetAddresses;

    /**
     * Used externally only for testing
     */
    NetworkInterfaceInfo(String name, boolean up, boolean virtual, boolean loopback, List<InetAddress> inetAddresses) {
        this.up = up;
        this.virtual = virtual;
        this.loopback = loopback;
        this.name = name;
        this.inetAddresses = inetAddresses;
    }

    public NetworkInterfaceInfo(NetworkInterface networkInterface) throws SocketException {
        this(networkInterface.getName(), networkInterface.isUp(), networkInterface.isVirtual(), networkInterface.isLoopback(),
                Collections.list(networkInterface.getInetAddresses()));
    }

    public boolean isUp() {
        return up;
    }

    public boolean isVirtual() {
        return virtual;
    }

    public boolean isLoopback() {
        return loopback;
    }

    public String getName() {
        return name;
    }

    public List<InetAddress> getInetAddresses() {
        return inetAddresses;
    }

    /**
     * Creates builder to build {@link NetworkInterfaceInfo}, used for testing.
     *
     * @return created builder
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * Builder to build {@link NetworkInterfaceInfo}.
     */
    public static final class Builder {

        private final String name;
        private boolean up = true;
        private boolean loopback;
        private boolean virtual;
        private String[] addresses = {};
        private InetAddress[] inetAddresses;

        private Builder(String name) {
            this.name = name;
        }

        public Builder withUp(boolean up) {
            this.up = up;
            return this;
        }

        public Builder withLoopback(boolean loopback) {
            this.loopback = loopback;
            return this;
        }

        public Builder withVirtual(boolean virtual) {
            this.virtual = virtual;
            return this;
        }

        public Builder withAddresses(String... addresses) {
            this.addresses = addresses;
            return this;
        }

        public Builder withInetAddresses(InetAddress... addresses) {
            this.inetAddresses = addresses;
            return this;
        }

        private static List<InetAddress> createInetAddresses(String... addresses) {
            List<InetAddress> inetAddresses = new ArrayList<>();
            for (String address : addresses) {
                try {
                    inetAddresses.add(InetAddress.getByName(address));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            return inetAddresses;
        }

        public NetworkInterfaceInfo build() {
            List<InetAddress> effectiveInetAddresses = this.inetAddresses == null
                    ? createInetAddresses(addresses) : Arrays.asList(this.inetAddresses);
            return new NetworkInterfaceInfo(name, up, virtual, loopback, effectiveInetAddresses);
        }
    }
}
