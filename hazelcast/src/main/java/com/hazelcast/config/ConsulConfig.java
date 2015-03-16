/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import static com.hazelcast.util.ValidationUtil.hasText;

 
public class ConsulConfig {

    private static final int CONNECTION_TIMEOUT = 5;

    private boolean enabled;
        
    private String name;

    private int connectionTimeoutSeconds = CONNECTION_TIMEOUT;

     /**
     * Returns the connection timeout.
     *
     * @return the connectionTimeoutSeconds
     * @see #setConnectionTimeoutSeconds(int)
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * Sets the connection timeout. This is the maximum amount of time Hazelcast will try to
     * connect to a well known member before giving up. Setting it to a too low value could mean that a
     * member is not able to connect to a cluster. Setting it as too high a value means that member startup
     * could slow down because of longer timeouts (e.g. when a well known member is not up).
     *
     * @param connectionTimeoutSeconds the connection timeout in seconds.
     * @return the updated TcpIpConfig
     * @throws IllegalArgumentException if connectionTimeoutSeconds is smaller than 0.
     * @see #getConnectionTimeoutSeconds()
     */
    public ConsulConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        if (connectionTimeoutSeconds < 0) {
            throw new IllegalArgumentException("connection timeout can't be smaller than 0");
        }
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    /**
     * Checks if the Consul join mechanism is enabled.
     *
     * @return true if enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables the Consul join mechanism.
     *
     * @param enabled true to enable the Consul join mechanism, false to disable
     * @return ConsulConfig the updated ConsulConfig config.
     */
    public ConsulConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }
 

    /**
     * Set the serviceName
     * <p/>
     * Each HazelcastInstance will try to connect to at least one of the members, to find all other members,
     * and create a cluster.
     *
     * @param name the service name to add.
     * @return the updated configuration.
     * @throws IllegalArgumentException if member is null or empty.
     */
    public ConsulConfig setName(String name) {
        String nameText = hasText(name, "name");
        this.name = nameText.trim();
        return this;
    }

    public String getName() {
        return name;
    }


    @Override
    public String toString() {
        return "Consul [enabled=" + enabled
                + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds
                + ", name=" + this.name
                + "]";
    }
}
