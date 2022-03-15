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

package com.hazelcast.client.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * A {@link ClientConfig} which is initialized by loading an XML configuration file from the classpath.
 *
 */
public class ClientClasspathXmlConfig extends ClientConfig {
    private static final ILogger LOGGER = Logger.getLogger(ClientClasspathXmlConfig.class);

    /**
     * Creates a config which is loaded from a classpath resource using the
     * {@link Thread#currentThread} contextClassLoader. The System.properties are used to resolve variables
     * in the XML.
     *
     * @param resource the resource, an XML configuration file from the classpath,
     *                 without the "classpath:" prefix
     * @throws IllegalArgumentException      if the resource could not be found
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public ClientClasspathXmlConfig(String resource) {
        this(resource, System.getProperties());
    }

    /**
     * Creates a config which is loaded from a classpath resource using the
     * {@link Thread#currentThread} contextClassLoader.
     *
     * @param resource   the resource, an XML configuration file from the classpath,
     *                   without the "classpath:" prefix
     * @param properties the Properties to resolve variables in the XML
     * @throws IllegalArgumentException      if the resource could not be found or if properties is {@code null}
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public ClientClasspathXmlConfig(String resource, Properties properties) {
        this(Thread.currentThread().getContextClassLoader(), resource, properties);
    }

    /**
     * Creates a config which is loaded from a classpath resource.
     *
     * @param classLoader the ClassLoader used to load the resource
     * @param resource    the resource, an XML configuration file from the classpath,
     *                    without the "classpath:" prefix
     * @param properties  the properties used to resolve variables in the XML
     * @throws IllegalArgumentException      if classLoader or resource is {@code null}, or if the resource is not found
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public ClientClasspathXmlConfig(ClassLoader classLoader, String resource, Properties properties) {
        checkTrue(classLoader != null, "classLoader can't be null");
        checkTrue(resource != null, "resource can't be null");
        checkTrue(properties != null, "properties can't be null");

        LOGGER.info("Configuring Hazelcast Client from '" + resource + "'.");
        InputStream in = classLoader.getResourceAsStream(resource);
        checkTrue(in != null, "Specified resource '" + resource + "' could not be found!");

        XmlClientConfigBuilder xmlClientConfigBuilder = new XmlClientConfigBuilder(in);
        xmlClientConfigBuilder.setProperties(properties);
        xmlClientConfigBuilder.build(this, classLoader);
    }
}
