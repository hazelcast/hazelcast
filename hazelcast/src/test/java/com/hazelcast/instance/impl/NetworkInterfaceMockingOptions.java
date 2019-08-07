package com.hazelcast.instance.impl;

import java.net.NetworkInterface;

import static com.hazelcast.util.Preconditions.checkNotNull;

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
