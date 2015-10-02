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

package com.hazelcast.hibernate.instance;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.StringUtil;
import org.hibernate.cache.CacheException;

import java.io.IOException;
import java.util.Properties;

/**
 * A factory implementation to build up a {@link com.hazelcast.core.HazelcastInstance}
 * implementation using {@link com.hazelcast.core.Hazelcast}.
 */
class HazelcastInstanceLoader implements IHazelcastInstanceLoader {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastInstanceFactory.class);

    private HazelcastInstance instance;
    private Config config;
    private boolean shutDown;

    @Override
    public void configure(Properties props) {
        String configResourcePath = CacheEnvironment.getConfigFilePath(props);
        if (!StringUtil.isNullOrEmptyAfterTrim(configResourcePath)) {
            try {
                this.config = ConfigLoader.load(configResourcePath);
            } catch (IOException e) {
                LOGGER.warning("IOException: " + e.getMessage());
            }
            if (config == null) {
                throw new CacheException("Could not find configuration file: " + configResourcePath);
            }
        } else {
            this.config = new XmlConfigBuilder().build();
        }

        String instanceName = CacheEnvironment.getInstanceName(props);

        if (instanceName != null) {
            config.setInstanceName(instanceName);
        }

        this.shutDown = CacheEnvironment.shutdownOnStop(props, (instanceName == null));

    }

    @Override
    public HazelcastInstance loadInstance() throws CacheException {
        if (config.getInstanceName() == null) {
            instance = Hazelcast.newHazelcastInstance(config);
        } else {
            instance = Hazelcast.getOrCreateHazelcastInstance(config);
        }
        return instance;
    }

    @Override
    public void unloadInstance() throws CacheException {
        if (instance == null) {
            return;
        }
        if (!shutDown) {
            LOGGER.warning(CacheEnvironment.SHUTDOWN_ON_STOP + " property is set to 'false'. "
                    + "Leaving current HazelcastInstance active! (Warning: Do not disable Hazelcast "
                    + GroupProperty.SHUTDOWNHOOK_ENABLED + " property!)");
            return;
        }
        try {
            instance.getLifecycleService().shutdown();
            instance = null;
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }
}
