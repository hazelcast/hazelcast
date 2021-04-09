/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spring.HazelcastClientBeanDefinitionParser;
import com.hazelcast.spring.HazelcastConfigBeanDefinitionParser;
import com.hazelcast.spring.HazelcastFailoverClientBeanDefinitionParser;

import java.util.function.Supplier;

/**
 * Provides factory methods for {@link Config} and {@link ClientConfig}.
 * This class is used in {@link HazelcastConfigBeanDefinitionParser},
 * {@link HazelcastClientBeanDefinitionParser} and
 * {@link HazelcastFailoverClientBeanDefinitionParser} to create
 * `empty` configuration instances. This factory can be used to
 * pre-configure the configuration instances, see
 * {@code CustomSpringJUnit4ClassRunner} for the usage.
 */
public final class ConfigFactory {

    private static volatile Supplier<Config> configSupplier = Config::new;
    private static volatile Supplier<ClientConfig> clientConfigSupplier = ClientConfig::new;
    private static volatile Supplier<ClientFailoverConfig> clientFailoverConfigSupplier = ClientFailoverConfig::new;

    private ConfigFactory() {
    }

    static void setConfigSupplier(Supplier<Config> configSupplier) {
        ConfigFactory.configSupplier = configSupplier;
    }

    static void setClientConfigSupplier(Supplier<ClientConfig> clientConfigSupplier) {
        ConfigFactory.clientConfigSupplier = clientConfigSupplier;
    }

    static void setClientFailoverConfigSupplier(Supplier<ClientFailoverConfig> clientFailoverConfigSupplier) {
        ConfigFactory.clientFailoverConfigSupplier = clientFailoverConfigSupplier;
    }

    public static Config newConfig() {
        return configSupplier.get();
    }

    public static ClientConfig newClientConfig() {
        return clientConfigSupplier.get();
    }

    public static ClientFailoverConfig newClientFailoverConfig() {
        return clientFailoverConfigSupplier.get();
    }

}
