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

public class FileSystemXmlConfig extends Config {

    private final ILogger logger = Logger.getLogger(FileSystemXmlConfig.class.getName());

    public FileSystemXmlConfig() {
    }

    public FileSystemXmlConfig(String configFilename) throws FileNotFoundException {
        this(new File(configFilename));
    }

    public FileSystemXmlConfig(File configFile) throws FileNotFoundException {
        super();
        logger.log(Level.INFO, "Configuring Hazelcast from '" + configFile.getAbsolutePath() + "'.");
        InputStream in = new FileInputStream(configFile);
        new XmlConfigBuilder(in).build(this);
    }
}
