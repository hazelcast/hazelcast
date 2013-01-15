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

import java.io.InputStream;
import java.util.logging.Level;

public class ClasspathXmlConfig extends Config {

    private final ILogger logger = Logger.getLogger(ClasspathXmlConfig.class.getName());

    public ClasspathXmlConfig() {
    }

    public ClasspathXmlConfig(String resource) {
        this(Thread.currentThread().getContextClassLoader(), resource);
    }

    public ClasspathXmlConfig(ClassLoader classLoader, String resource) {
        super();
        logger.log(Level.INFO, "Configuring Hazelcast from '" + resource + "'.");
        InputStream in = classLoader.getResourceAsStream(resource);
        if (in == null) {
            throw new NullPointerException("Specified resource '" + resource + "' could not be found!");
        }
        new XmlConfigBuilder(in).build(this);
    }
}
