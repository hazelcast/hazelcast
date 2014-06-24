/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * Creates a {@link Config} loaded from an in memory Hazelcast XML String.
 */
public class InMemoryXmlConfig extends Config {

    private static final ILogger LOGGER = Logger.getLogger(InMemoryXmlConfig.class);

    /**
     * Creates a Config from the provided XML and uses the System.properties to resolve variables
     * in the XML.
     *
     * @param xml the XML content
     * @throws IllegalArgumentException              if the XML is null or empty.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public InMemoryXmlConfig(String xml) {
        this(xml, System.getProperties());
    }

    /**
     * Creates a Config from the provided XML and properties to resolve the variables in the XML.
     *
     * @param xml the XML content
     * @throws IllegalArgumentException              if the XML is null or empty or if properties is null.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public InMemoryXmlConfig(String xml, Properties properties) {
        LOGGER.info("Configuring Hazelcast from 'in-memory xml'.");
        if (xml == null || "".equals(xml.trim())) {
            throw new IllegalArgumentException("XML configuration is null or empty! Please use a well-structured xml.");
        }
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }

        InputStream in = new ByteArrayInputStream(stringToBytes(xml));
        new XmlConfigBuilder(in).setProperties(properties).build(this);
    }
}
