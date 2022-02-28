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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.sort;

/**
 * A {@link DiagnosticsPlugin} that displays the {@link Config#getProperties()}.
 */
public class ConfigPropertiesPlugin extends DiagnosticsPlugin {

    private final HazelcastProperties properties;
    private final List<String> keyList = new ArrayList<String>();

    public ConfigPropertiesPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(ConfigPropertiesPlugin.class), nodeEngine.getProperties());
    }

    public ConfigPropertiesPlugin(ILogger logger, HazelcastProperties properties) {
        super(logger);
        this.properties = properties;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active");
    }

    @Override
    public long getPeriodMillis() {
        return STATIC;
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        keyList.clear();
        keyList.addAll(properties.keySet());
        sort(keyList);

        writer.startSection("ConfigProperties");
        for (String key : keyList) {
            String value = properties.get(key);
            writer.writeKeyValueEntry(key, value);
        }
        writer.endSection();
    }
}
