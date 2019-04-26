/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.jet.config.JetConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Locates and loads Jet or Jet Client configurations from various locations.
 *
 * @see XmlJetConfigLocator
 * @see YamlJetConfigLocator
 */
public final class ConfigProvider {

    private ConfigProvider() {
    }

    @Nonnull
    public static JetConfig locateAndGetJetConfig(@Nullable Properties properties) {
        XmlJetConfigLocator xmlConfigLocator = new XmlJetConfigLocator();
        YamlJetConfigLocator yamlConfigLocator = new YamlJetConfigLocator();
        JetConfig config;
        if (xmlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading XML config if provided in system property
            config = new XmlJetConfigBuilder(xmlConfigLocator).setProperties(properties).build();

        } else if (yamlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading YAML config if provided in system property
            config = new YamlJetConfigBuilder(yamlConfigLocator).setProperties(properties).build();

        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading XML config from the working directory or from the classpath
            config = new XmlJetConfigBuilder(xmlConfigLocator).setProperties(properties).build();

        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading YAML config from the working directory or from the classpath
            config = new YamlJetConfigBuilder(yamlConfigLocator).setProperties(properties).build();

        } else {
            // 5. Loading the default XML configuration file
            xmlConfigLocator.locateDefault();
            config = new XmlJetConfigBuilder(xmlConfigLocator).setProperties(properties).build();
        }
        return config;

    }

    @Nonnull
    public static JetConfig locateAndGetJetConfig() {
        return locateAndGetJetConfig(null);
    }

    @Nonnull
    public static ClientConfig locateAndGetClientConfig() {
        ClientConfig config;
        XmlJetClientConfigLocator xmlConfigLocator = new XmlJetClientConfigLocator();
        YamlJetClientConfigLocator yamlConfigLocator = new YamlJetClientConfigLocator();

        if (xmlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading config if provided in system property and it is an XML file
            config = new XmlClientConfigBuilder(xmlConfigLocator.getIn()).build();

        } else if (yamlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading config if provided in system property and it is an YAML file
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();

        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading XML config from the working directory or from the classpath
            config = new XmlClientConfigBuilder(xmlConfigLocator.getIn()).build();

        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading YAML config from the working directory or from the classpath
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();

        } else {
            // 5. Loading the default XML configuration file
            xmlConfigLocator.locateDefault();
            config = new XmlClientConfigBuilder(xmlConfigLocator.getIn()).build();
        }
        return config;
    }

}
