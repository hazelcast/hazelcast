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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.datalink.HazelcastDataLink.CLIENT_XML;
import static com.hazelcast.datalink.HazelcastDataLink.CLIENT_XML_PATH;
import static com.hazelcast.datalink.HazelcastDataLink.CLIENT_YAML_PATH;
import static com.hazelcast.datalink.HazelcastDataLink.CLIENT_YML;

public class HazelcastDataLinkFileReader {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastDataLinkFileReader.class);

    public void readFilePathIfProvided(DataLinkConfig dataLinkConfig) {
        String clientXmlPath = dataLinkConfig.getProperty(CLIENT_XML_PATH);
        if (readFile(dataLinkConfig, clientXmlPath, CLIENT_XML)) {
            LOGGER.info("Successfully read XML file :" + clientXmlPath);
        } else {
            String clientYamlPath = dataLinkConfig.getProperty(CLIENT_YAML_PATH);
            if (readFile(dataLinkConfig, clientYamlPath, CLIENT_YML)) {
                LOGGER.info("Successfully read YAML file :" + clientYamlPath);
            }
        }
    }

    private boolean readFile(DataLinkConfig dataLinkConfig, String filePath, String propertyKey) {
        boolean result = false;
        if (!StringUtil.isNullOrEmpty(filePath)) {
            String fileContent = readFileContent(filePath);
            dataLinkConfig.setProperty(propertyKey, fileContent);
            result = true;
        }
        return result;
    }

    private String readFileContent(String filePath) {
        try {
            Path path = Paths.get(filePath);
            byte[] bytes = Files.readAllBytes(path);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new HazelcastException("Unable to read file :" + filePath);
        }
    }
}
