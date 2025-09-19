/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.sort;

/**
 * A {@link DiagnosticsPlugin} that displays the {@link Config#getProperties()}.
 */
public class ConfigPropertiesPlugin extends DiagnosticsPlugin {

    private final HazelcastProperties properties;
    private final List<String> keyList = new ArrayList<>();
    private NodeEngineImpl nodeEngine;

    public ConfigPropertiesPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(ConfigPropertiesPlugin.class),
                nodeEngine.getProperties());
        this.nodeEngine = nodeEngine;
    }

    public ConfigPropertiesPlugin(ILogger logger, HazelcastProperties properties) {
        super(logger);
        this.properties = properties;
    }

    @Override
    public void onStart() {
        super.onStart();
        logger.info("Plugin:active");
    }

    @Override
    void readProperties() {
        // no properties to read
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        logger.info("Plugin:inactive");
    }

    @Override
    public long getPeriodMillis() {
        return RUN_ONCE_PERIOD_MS;
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        if (!isActive()) {
            return;
        }
        keyList.clear();
        keyList.addAll(properties.keySet());
        sort(keyList);

        writer.startSection("ConfigProperties");
        for (String key : keyList) {
            String value = properties.get(key);
            writer.writeKeyValueEntry(key, value);
        }

        // If diagnostics belong to client, then there is no nodeEngine.
        if (nodeEngine != null) {
            Map<String, String> pluginProperties = nodeEngine.getDiagnostics().getDiagnosticsConfig().getPluginProperties();
            for (Map.Entry<String, String> entry : pluginProperties.entrySet()) {
                writer.writeKeyValueEntry(entry.getKey(), entry.getValue());
            }
        }

        writer.endSection();
    }
}
