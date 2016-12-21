/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.config;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.impl.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * A support class for the {@link XmlJetConfigBuilder} to locate the client
 * xml configuration.
 */
final class XmlJetConfigLocator {
    public static final String HAZELCAST_JET_CONFIG_PROPERTY = "hazelcast.jet.config";

    private static final ILogger LOGGER = Logger.getLogger(XmlJetConfigLocator.class);
    private static final String HAZELCAST_JET_XML = "hazelcast-jet.xml";
    private static final String HAZELCAST_JET_DEFAULT_XML = "hazelcast-jet-default.xml";

    private XmlJetConfigLocator() {
    }

    /**
     * Constructs a XmlJetConfigLocator.
     *
     * @throws com.hazelcast.core.HazelcastException if the client XML config is not located.
     */
    public static InputStream getConfigStream(Properties properties) {
        try {
            return Stream.<Callable<InputStream>>of(
                    () -> fromProperties(properties),
                    XmlJetConfigLocator::fromWorkingDirectory,
                    XmlJetConfigLocator::fromClasspath,
                    XmlJetConfigLocator::defaultFromClasspath)
                    .map(Util::uncheckCall)
                    .filter(Objects::nonNull)
                    .findFirst().get();
        } catch (Exception e) {
            throw new HazelcastException("Failed to initialize Jet configuration", e);
        }
    }

    private static InputStream defaultFromClasspath() throws IOException {
        LOGGER.info("Loading " + HAZELCAST_JET_DEFAULT_XML + " from classpath.");

        InputStream in = Config.class.getClassLoader().getResourceAsStream(HAZELCAST_JET_DEFAULT_XML);
        if (in == null) {
            throw new IOException("Could not load " + HAZELCAST_JET_DEFAULT_XML + " + from classpath");
        }
        return in;
    }

    private static InputStream fromClasspath() throws IOException {
        URL url = Config.class.getClassLoader().getResource(HAZELCAST_JET_XML);
        if (url == null) {
            LOGGER.finest("Could not find " + HAZELCAST_JET_XML + " in classpath.");
            return null;
        }

        LOGGER.info("Loading " + HAZELCAST_JET_XML + " from classpath.");

        InputStream in = Config.class.getClassLoader().getResourceAsStream(HAZELCAST_JET_XML);
        if (in == null) {
            throw new IOException("Could not load " + HAZELCAST_JET_XML + " from classpath");
        }
        return in;
    }

    private static InputStream fromWorkingDirectory() throws IOException {
        File file = new File(HAZELCAST_JET_XML);
        if (!file.exists()) {
            LOGGER.finest("Could not find " + HAZELCAST_JET_XML + " in working directory.");
            return null;
        }

        LOGGER.info("Loading " + HAZELCAST_JET_XML + " from working directory.");
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new IOException("Failed to open file: " + file.getAbsolutePath(), e);
        }
    }

    private static InputStream fromProperties(Properties properties) throws IOException {
        String path = properties.getProperty(HAZELCAST_JET_CONFIG_PROPERTY);

        if (path == null) {
            LOGGER.finest("Could not find property " + HAZELCAST_JET_CONFIG_PROPERTY);
            return null;
        }

        LOGGER.info("Loading configuration " + path + " from property " + HAZELCAST_JET_CONFIG_PROPERTY);

        if (path.startsWith("classpath:")) {
            return loadPropertyClassPathResource(path);
        } else {
            return loadPropertyFileResource(path);
        }
    }

    private static InputStream loadPropertyFileResource(String path) throws IOException {
        //it is a file.
        File configurationFile = new File(path);
        LOGGER.info("Using configuration file at " + configurationFile.getAbsolutePath());

        if (!configurationFile.exists()) {
            String msg = "Config file at " + configurationFile.getAbsolutePath() + " doesn't exist.";
            throw new FileNotFoundException(msg);
        }

        return new FileInputStream(configurationFile);
    }

    private static InputStream loadPropertyClassPathResource(String path) throws IOException {
        //it is a explicit configured classpath resource.
        String resource = path.substring("classpath:".length());

        LOGGER.info("Using classpath resource at " + resource);

        InputStream in = Config.class.getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new IOException("Could not load classpath resource: " + resource);
        }
        return in;
    }

}
