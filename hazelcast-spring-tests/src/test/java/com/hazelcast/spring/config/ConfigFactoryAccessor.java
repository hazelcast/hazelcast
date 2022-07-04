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

package com.hazelcast.spring.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.config.Config;

import java.util.function.Supplier;

/**
 * Provides accessors for
 * {@link ConfigFactory#setConfigSupplier(Supplier)},
 * {@link ConfigFactory#setClientConfigSupplier(Supplier)} and
 * {@link ConfigFactory#setClientFailoverConfigSupplier(Supplier)}.
 */
public final class ConfigFactoryAccessor {

    private ConfigFactoryAccessor() {
    }

    public static void setConfigSupplier(Supplier<Config> configSupplier) {
        ConfigFactory.setConfigSupplier(configSupplier);
    }

    public static void setClientConfigSupplier(Supplier<ClientConfig> clientConfigSupplier) {
        ConfigFactory.setClientConfigSupplier(clientConfigSupplier);
    }

    public static void setClientFailoverConfigSupplier(Supplier<ClientFailoverConfig> clientFailoverConfigSupplier) {
        ConfigFactory.setClientFailoverConfigSupplier(clientFailoverConfigSupplier);
    }

}
