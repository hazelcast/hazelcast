/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitors;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.sort;

/**
 * A {@link PerformanceMonitorPlugin} that displays the {@link Config#getProperties()}
 */
public class ConfigPropertiesPlugin extends PerformanceMonitorPlugin {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    public ConfigPropertiesPlugin(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ConfigPropertiesPlugin.class);
    }

    @Override
    public long getPeriodMillis() {
        return STATIC;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active");
    }

    @Override
    public void run(PerformanceLogWriter writer) {
        Config config = nodeEngine.getConfig();
        Properties properties = config.getProperties();

        List keys = new LinkedList();
        keys.addAll(properties.keySet());
        sort(keys);

        writer.startSection("ConfigProperties");
        for (Object key : keys) {
            String keyString = (String) key;
            String value = properties.getProperty(keyString);
            writer.writeKeyValueEntry(keyString, value);
        }
        writer.endSection();
    }
}
