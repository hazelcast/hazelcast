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

import com.hazelcast.config.AbstractYamlConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.config.yaml.YamlDomChecker;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import org.w3c.dom.Node;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.config.yaml.W3cDomUtil.asW3cNode;
import static com.hazelcast.util.Preconditions.checkTrue;

public class YamlJetConfigBuilder extends AbstractYamlConfigBuilder {

    private final InputStream in;

    public YamlJetConfigBuilder() {
        this((YamlJetConfigLocator) null);
    }

    public YamlJetConfigBuilder(YamlJetConfigLocator locator) {
        if (locator == null) {
            locator = new YamlJetConfigLocator(true);
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
    }

    public YamlJetConfigBuilder(InputStream inputStream) {
        checkTrue(inputStream != null, "inputStream can't be null");
        this.in = inputStream;
    }

    @Override
    protected String getConfigRoot() {
        return JetConfigSections.HAZELCAST_JET.name;
    }

    public JetConfig build() {
        return build(new JetConfig());
    }

    public JetConfig build(JetConfig config) {
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        config.setHazelcastConfig(getMemberConfig(getProperties()));
        return config;
    }

    private void parseAndBuildConfig(JetConfig config) throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = ((YamlMapping) YamlLoader.load(in));
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        }

        YamlNode jetRoot = yamlRootNode.childAsMapping(JetConfigSections.HAZELCAST_JET.name);
        if (jetRoot == null) {
            throw new InvalidConfigurationException("No mapping with hazelcast-jet key is" +
                    " found in the provided configuration");
        }

        YamlDomChecker.check(jetRoot);

        Node w3cRootNode = asW3cNode(jetRoot);
        replaceVariables(w3cRootNode);
        importDocuments(jetRoot);

        new YamlJetDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }


    public YamlJetConfigBuilder setProperties(@Nullable Properties properties) {
        if (properties == null) {
            properties = System.getProperties();
        }
        setPropertiesInternal(properties);
        return this;
    }

    private static Config getMemberConfig(Properties properties) {
        YamlJetMemberConfigLocator locator = new YamlJetMemberConfigLocator();
        locator.locateEverywhere();
        return new YamlConfigBuilder(locator.getIn()).setProperties(properties).build();
    }


}
