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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.logging.Level;


/**
 * A {@link Config} which is  which includes functionality for loading itself from a
 * XML configuration file.
 */
public class FileSystemXmlConfig extends Config {

    private final static ILogger logger = Logger.getLogger(FileSystemXmlConfig.class);

    /**
     * Creates a Config based on some Hazelcast xml file.
     *
     * @param configFilename the path of the file.
     * @throws FileNotFoundException fi the file is not found.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public FileSystemXmlConfig(String configFilename) throws FileNotFoundException {
        this(new File(configFilename));
    }

    /**
     * Creates a Config based on a Hazelcast xml file.
     *
     * @param configFile the configuration file.
     * @throws FileNotFoundException if the file doesn't exist.
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    public FileSystemXmlConfig(File configFile) throws FileNotFoundException {
        logger.info("Configuring Hazelcast from '" + configFile.getAbsolutePath() + "'.");
        InputStream in = new FileInputStream(configFile);
        new XmlConfigBuilder(in).build(this);
    }
}
