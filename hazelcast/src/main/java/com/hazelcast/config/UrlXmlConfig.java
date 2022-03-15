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

package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * A {@link Config} which is loaded using some url pointing to a Hazelcast XML file.
 * <p>
 * Unlike {@link Config#load()} and its variants, a configuration constructed via
 * {@code UrlXmlConfig} does not apply overrides found in environment variables/system properties.
 */
public class UrlXmlConfig extends Config {

    private static final ILogger LOGGER = Logger.getLogger(UrlXmlConfig.class);

    /**
     * Creates new Config which is loaded from the given url and uses the System.properties to replace
     * variables in the XML.
     *
     * @param url the url pointing to the Hazelcast XML file
     * @throws MalformedURLException         if the url is not correct
     * @throws IOException                   if something fails while loading the resource
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public UrlXmlConfig(String url) throws IOException {
        this(new URL(url));
    }

    /**
     * Creates new Config which is loaded from the given url.
     *
     * @param url        the url pointing to the Hazelcast XML file
     * @param properties the properties for replacing variables
     * @throws IllegalArgumentException      if properties is {@code null}
     * @throws MalformedURLException         if the url is not correct
     * @throws IOException                   if something fails while loading the resource
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public UrlXmlConfig(String url, Properties properties) throws IOException {
        this(new URL(url), properties);
    }

    /**
     * Creates new Config which is loaded from the given url and uses the System.properties to replace
     * variables in the XML.
     *
     * @param url the URL pointing to the Hazelcast XML file
     * @throws IOException                   if something fails while loading the resource
     * @throws IllegalArgumentException      if the url is {@code null}
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public UrlXmlConfig(URL url) throws IOException {
        this(url, System.getProperties());
    }

    /**
     * Creates new Config which is loaded from the given url.
     *
     * @param url        the URL pointing to the Hazelcast XML file
     * @param properties the properties for replacing variables
     * @throws IOException                   if something fails while loading the resource
     * @throws IllegalArgumentException      if the url or properties is {@code null}
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    public UrlXmlConfig(URL url, Properties properties) throws IOException {
        checkTrue(url != null, "url can't be null");
        checkTrue(properties != null, "properties can't be null");

        LOGGER.info("Configuring Hazelcast from '" + url.toString() + "'.");
        InputStream in = url.openStream();
        new XmlConfigBuilder(in).setProperties(properties).build(this);
    }
}
