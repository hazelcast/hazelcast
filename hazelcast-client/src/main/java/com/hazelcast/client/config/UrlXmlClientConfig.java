/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class UrlXmlClientConfig extends ClientConfig {

    private static final ILogger LOGGER = Logger.getLogger(UrlXmlClientConfig.class);

    /**
     * Creates new Config which is loaded from the given url and uses the System.properties to replace
     * variables in the XML.
     *
     * @param url the url pointing to the Hazelcast XML file.
     * @throws java.net.MalformedURLException  if the url is not correct
     * @throws java.io.IOException if something fails while loading the resource.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public UrlXmlClientConfig(String url) throws IOException {
        this(new URL(url));
    }

    /**
     * Creates new Config which is loaded from the given url and uses the System.properties to replace
     * variables in the XML.
     *
     * @param url the URL pointing to the Hazelcast XML file.
     * @throws java.io.IOException if something fails while loading the resource
     * @throws IllegalArgumentException if the url is null.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public UrlXmlClientConfig(URL url) throws IOException {
        if (url == null) {
            throw new IllegalArgumentException("url can't be null");
        }

        LOGGER.info("Configuring HazelcastClient from '" + url.toString() + "'.");
        InputStream in = url.openStream();
        new XmlClientConfigBuilder(in).build(this);
    }
}
