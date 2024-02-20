/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc;

import com.hazelcast.config.Config;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.internal.tpcengine.TpcEngine;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.List;

/**
 * Responsible for configuring the TPC system.
 */
public interface TpcServerBootstrap {

    /**
     * If set, overrides {@link com.hazelcast.config.tpc.TpcConfig#isEnabled()}
     */
    HazelcastProperty TPC_ENABLED = new HazelcastProperty(
            "hazelcast.internal.tpc.enabled");

    /**
     * If set, overrides {@link com.hazelcast.config.tpc.TpcConfig#getEventloopCount()}
     */
    HazelcastProperty TPC_EVENTLOOP_COUNT = new HazelcastProperty(
            "hazelcast.internal.tpc.eventloop.count");

    /**
     * Checks if TPC is enabled by checking the System properties and the
     * config.
     *
     * @param config the config
     * @return true if TPC was enabled, or false otherwise.
     */
    static boolean loadTpcEnabled(Config config) {
        String s = System.getProperty(TpcServerBootstrap.TPC_ENABLED.getName());
        if (s != null) {
            return Boolean.parseBoolean(s);
        }

        s = config.getProperties().getProperty(TPC_ENABLED.getName());
        if (s != null) {
            return Boolean.parseBoolean(s);
        }

        return config.getTpcConfig().isEnabled();
    }

    /**
     * Starts the TpcServerBootstrap. If TPC is not enabled, call is ignored.
     */
    void start();

    /**
     * Shutdown the TpcServerBootstrap and waits for termination. If TPC is not
     * enabled, call is ignored.
     */
    void shutdown();

    /**
     * Checks if TPC is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    boolean isEnabled();

    /**
     * Returns the number of eventloops.
     *
     * @return the number of eventloops. If TPC isn't enabled, 0 is returned.
     */
    int eventloopCount();

    /**
     * Returns the client ports a client can connect to.
     *
     * @return the list of client ports. An empty list will be returned if TPC is disabled.
     */
    List<Integer> getClientPorts();

    /**
     * Returns the TpcEngine.
     *
     * @return the TPCEngine or null if TPC disabled.
     */
    TpcEngine getTpcEngine();

    /**
     * Returns the TpcSocketConfig.
     *
     * @return the TpcSocketConfig.
     */
    TpcSocketConfig getClientSocketConfig();
}
