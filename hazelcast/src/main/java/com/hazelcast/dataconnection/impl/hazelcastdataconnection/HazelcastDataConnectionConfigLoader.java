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

package com.hazelcast.dataconnection.impl.hazelcastdataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.HazelcastDataConnection;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Loads the DataConnectionConfig if file path is provided
 */
public class HazelcastDataConnectionConfigLoader {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastDataConnectionConfigLoader.class);

    public DataConnectionConfig load(DataConnectionConfig dataConnectionConfig) {
        // Make a copy to preserve the original configuration
        DataConnectionConfig loadedConfig = new DataConnectionConfig(dataConnectionConfig);

        // Try XML file first
        if (loadConfig(loadedConfig, HazelcastDataConnection.CLIENT_XML_PATH, HazelcastDataConnection.CLIENT_XML)) {
            return loadedConfig;
        }
        // Try YML file
        loadConfig(loadedConfig, HazelcastDataConnection.CLIENT_YML_PATH, HazelcastDataConnection.CLIENT_YML);
        return loadedConfig;
    }

    private boolean loadConfig(DataConnectionConfig dataConnectionConfig, String property, String propertyKey) {
        boolean result = false;
        String filePath = dataConnectionConfig.getProperty(property);
        if (!StringUtil.isNullOrEmpty(filePath)) {
            String fileContent = readFileContent(filePath);
            dataConnectionConfig.setProperty(propertyKey, fileContent);
            LOGGER.info("Successfully read file: " + filePath);
            result = true;
        }
        return result;
    }

    private String readFileContent(String filePath) {
        try {
            Path path = Paths.get(filePath);
            return Files.readString(path);
        } catch (IOException exception) {
            throw new HazelcastException("Unable to read file :" + filePath);
        }
    }
}
