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

public class UrlXmlConfig extends Config {

    private final ILogger logger = Logger.getLogger(UrlXmlConfig.class.getName());

    public UrlXmlConfig() {
    }

    public UrlXmlConfig(String url) throws MalformedURLException, IOException {
        this(new URL(url));
    }

    public UrlXmlConfig(URL url) throws IOException {
        super();
        logger.log(Level.INFO, "Configuring Hazelcast from '" + url.toString() + "'.");
        InputStream in = url.openStream();
        new XmlConfigBuilder(in).build(this);
    }
}
