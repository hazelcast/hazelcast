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

/**
 * Contains client configurations for TPC.
 * <p>
 * TPC is the next generation Hazelcast that uses thread-per-core model.
 * <p>
 * TPC-aware clients will maintain connections to all cores of all cluster
 * members. The client will route partition-specific invocations to the
 * correct core of the correct member in the best effort basis.
 *
 * @since 5.3
 */
@Beta
public final class ClientTpcConfig {

    private boolean enabled = Boolean.parseBoolean(
            System.getProperty("hazelcast.client.tpc.enabled", "false"));

    public ClientTpcConfig() {
    }

    public ClientTpcConfig(@Nonnull ClientTpcConfig tpcConfig) {
        this.enabled = tpcConfig.enabled;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientTpcConfig that = (ClientTpcConfig) o;
        return enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled);
    }

    @Override
    public String toString() {
        return "ClientTpcConfig{"
                + "enabled=" + enabled
                + '}';
    }
}
