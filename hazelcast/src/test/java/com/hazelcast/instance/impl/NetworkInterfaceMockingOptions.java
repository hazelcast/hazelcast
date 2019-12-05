/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import java.net.NetworkInterface;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration object for {@link NetworkInterface} mocking.
 */
public class NetworkInterfaceMockingOptions {

    final String name;
    final boolean up;
    final boolean loopback;
    final boolean virtual;
    final String[] addresses;

    private NetworkInterfaceMockingOptions(Builder builder) {
        this.name = checkNotNull(builder.name);
        this.up = builder.up;
        this.loopback = builder.loopback;
        this.virtual = builder.virtual;
        this.addresses = checkNotNull(builder.addresses);
    }

    /**
     * Creates builder to build {@link NetworkInterfaceMockingOptions}.
     *
     * @return created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to build {@link NetworkInterfaceMockingOptions}.
     */
    @SuppressWarnings("SameParameterValue")
    public static final class Builder {

        private String name;
        private boolean up = true;
        private boolean loopback = false;
        private boolean virtual = false;
        private String[] addresses = {};

        private Builder() {
        }

        Builder withName(String name) {
            this.name = name;
            return this;
        }

        Builder withUp(boolean up) {
            this.up = up;
            return this;
        }

        Builder withLoopback(boolean loopback) {
            this.loopback = loopback;
            return this;
        }

        Builder withVirtual(boolean virtual) {
            this.virtual = virtual;
            return this;
        }

        Builder withAddresses(String... addresses) {
            this.addresses = addresses;
            return this;
        }

        NetworkInterfaceMockingOptions build() {
            return new NetworkInterfaceMockingOptions(this);
        }
    }
}
