/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * Contains client configurations for TPC.
 * <p>
 * TPC is the next generation Hazelcast that uses thread-per-core model.
 * <p>
 * TPC-aware clients will connect to the TPC ports depending on the configured
 * number of connections.
 *
 * @since 5.3
 */
@Beta
public final class ClientTpcConfig {

    private boolean enabled;

    private int connectionCount;

    public ClientTpcConfig() {
        setEnabled(Boolean.parseBoolean(System.getProperty("hazelcast.client.tpc.enabled", "false")));
        setConnectionCount(Integer.getInteger("hazelcast.client.tpc.connectionCount", 1));
    }

    public ClientTpcConfig(@Nonnull ClientTpcConfig tpcConfig) {
        this.enabled = tpcConfig.enabled;
        this.connectionCount = tpcConfig.connectionCount;
    }

    /**
     * Returns if the TPC-aware mode is enabled.
     *
     * @return {@code true} if the TPC-aware mode is enabled, {@code false} otherwise.
     * @since 5.3
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables the TPC-aware mode.
     * <p>
     * When enabled, the configuration option set by the
     * {@link ClientNetworkConfig#setSmartRouting(boolean)} is ignored.
     *
     * @param enabled flag to enable or disable TPC-aware mode
     * @return this configuration for chaining.
     * @since 5.3
     */
    public ClientTpcConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Sets the number of connections to TPC ports offered by a Hazelcast member.
     *
     * <ol>
     *     <li>If set to a negative value, an IllegalArgumentException will be
     *     thrown.</li>
     *     <li>If set to 0, the client will connect to every TPC port.
     *     </li>
     *     <li>If set to the same number as returned by the server, the client
     *     will connect to every TPC port.
     *     </li>
     *     <li>If set to a number larger than 0 and smaller than the number of
     *     returned TPC ports, the client will randomize the list of ports and
     *     make the configured number of connections.
     *     </li>
     *     <li>If set to a number larger than the number of TPC ports, the client
     *     will connect to each tpc port (has same effect as configuring 0)
     *     </li>
     * </ol>
     * Increasing the number of connections leads to more packets with a smaller
     * payload and this can lead to a performance penalty. Also in cloud environments
     * e.g. AWS there can be a packets per second limit (pps) and it pretty easy
     * to run into this limit if an equal number of connections is created as
     * TPC ports on the server and a lot of small interactions are done e.g. a
     * map.get with small payloads.
     *
     * @param connectionCount throws IllegalArgumentException when connectionCount
     *                        is negative.
     * @since 5.4
     */
    public ClientTpcConfig setConnectionCount(int connectionCount) {
        this.connectionCount = checkNotNegative(connectionCount, "connectionCount");
        return this;
    }

    /**
     * Gets the connection count.
     *
     * @return the number of connections.
     * @since 5.4
     */
    public int getConnectionCount() {
        return connectionCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientTpcConfig that = (ClientTpcConfig) o;
        if (that.enabled != this.enabled) {
            return false;
        }

        if (that.connectionCount != this.connectionCount) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled) + 31 * connectionCount;
    }

    @Override
    public String toString() {
        return "ClientTpcConfig{"
                + "enabled=" + enabled
                + ", connectionCount=" + connectionCount
                + '}';
    }
}
