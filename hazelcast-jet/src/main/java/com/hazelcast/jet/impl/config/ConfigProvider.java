/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.jet.config.JetConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Properties;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.validateSuffixInSystemProperty;
import static com.hazelcast.jet.impl.config.JetDeclarativeConfigUtil.SYSPROP_JET_CONFIG;

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
    public static JetConfig locateAndGetJetConfig() {
        return locateAndGetJetConfig(null);
    }

    @Nonnull
    public static JetConfig locateAndGetJetConfig(@Nullable Properties properties) {
        validateSuffixInSystemProperty(SYSPROP_JET_CONFIG);

        XmlJetConfigLocator xmlConfigLocator = new XmlJetConfigLocator();
        YamlJetConfigLocator yamlConfigLocator = new YamlJetConfigLocator();
        JetConfig config;

        if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading YAML config if provided in system property
            config = new YamlJetConfigBuilder(yamlConfigLocator).setProperties(properties).build();

        } else if (xmlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading XML config if provided in system property
            config = new XmlJetConfigBuilder(xmlConfigLocator).setProperties(properties).build();

        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading YAML config from the working directory or from the classpath
            config = new YamlJetConfigBuilder(yamlConfigLocator).setProperties(properties).build();

        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading XML config from the working directory or from the classpath
            config = new XmlJetConfigBuilder(xmlConfigLocator).setProperties(properties).build();

        } else {
            // 5. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config = new YamlJetConfigBuilder(yamlConfigLocator).setProperties(properties).build();
        }
        return config;

    }

    @Nonnull
    public static ClientConfig locateAndGetClientConfig() {
        validateSuffixInSystemProperty(SYSPROP_CLIENT_CONFIG);

        ClientConfig config;
        XmlJetClientConfigLocator xmlConfigLocator = new XmlJetClientConfigLocator();
        YamlJetClientConfigLocator yamlConfigLocator = new YamlJetClientConfigLocator();

        if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading config if provided in system property and it is an YAML file
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();

        } else if (xmlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading config if provided in system property and it is an XML file
            config = new XmlClientConfigBuilder(xmlConfigLocator.getIn()).build();

        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading YAML config from the working directory or from the classpath
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();

        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading XML config from the working directory or from the classpath
            config = new XmlClientConfigBuilder(xmlConfigLocator.getIn()).build();

        } else {
            // 5. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();
        }
        return config;
    }


    @Nonnull
    public static Config locateAndGetMemberConfig(@Nullable Properties properties) {
        validateSuffixInSystemProperty(SYSPROP_MEMBER_CONFIG);

        Config config;
        XmlJetMemberConfigLocator xmlConfigLocator = new XmlJetMemberConfigLocator();
        YamlJetMemberConfigLocator yamlConfigLocator = new YamlJetMemberConfigLocator();

        if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading config if provided in system property and it is an YAML file
            config = new YamlConfigBuilder(yamlConfigLocator.getIn()).setProperties(properties).build();

        } else if (xmlConfigLocator.locateFromSystemProperty()) {
            // 2. Try loading config if provided in system property and it is an XML file
            config = new XmlConfigBuilder(xmlConfigLocator.getIn()).setProperties(properties).build();

        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 3. Try loading YAML config from the working directory or from the classpath
            config = new YamlConfigBuilder(yamlConfigLocator.getIn()).setProperties(properties).build();

        } else if (xmlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 4. Try loading XML config from the working directory or from the classpath
            config = new XmlConfigBuilder(xmlConfigLocator.getIn()).setProperties(properties).build();

        } else {
            // 5. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config = new YamlConfigBuilder(yamlConfigLocator.getIn()).setProperties(properties).build();
        }
        return config;
    }

}
