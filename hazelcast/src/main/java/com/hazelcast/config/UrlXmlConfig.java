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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;

/**
 * A {@link Config} which is loaded using some url pointing to a Hazelcast XML file.
 */
public class UrlXmlConfig extends Config {

    private final static ILogger logger = Logger.getLogger(UrlXmlConfig.class);

    /**
     * Creates new Config which is loaded from the given url.
     *
     * @param url the url pointing to the Hazelcast XML file.
     * @throws MalformedURLException  if the url is not correct
     * @throws IOException if something fails while loading the resource.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public UrlXmlConfig(String url) throws MalformedURLException, IOException {
        this(new URL(url));
    }

    /**
     * Creates new Config which is loaded from the given url.
     *
     * @param url the URL pointing to the Hazelcast XML file.
     * @throws IOException if something fails while loading the resource
     * @throws IllegalArgumentException if the url is null.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public UrlXmlConfig(URL url) throws IOException {
        if(url == null){
            throw new IllegalArgumentException("url can't be null");
        }

        logger.info("Configuring Hazelcast from '" + url.toString() + "'.");
        InputStream in = url.openStream();
        new XmlConfigBuilder(in).build(this);
    }
}
