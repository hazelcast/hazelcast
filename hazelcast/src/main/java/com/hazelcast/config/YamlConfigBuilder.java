/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.yaml.W3cDomUtil;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.util.ExceptionUtil;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A YAML {@link ConfigBuilder} implementation.
 *
 * This config builder is compatible with the YAML 1.2 specification and
 * supports the JSON Scheme.
 */
public class YamlConfigBuilder implements ConfigBuilder {
    private final InputStream in;

    private File configurationFile;
    private URL configurationUrl;

    //    static {
    //        try {
    //            Config.class.forName("org.snakeyaml.engine.v1.api.Load");
    //        } catch (ClassNotFoundException e) {
    //            LOGGER.severe("The SnakeYAML engine library couldn't be found on the classpath");
    //        }
    //    }
    //

    /**
     * Constructs a YamlConfigBuilder that reads from the provided YAML file.
     *
     * @param YamlFileName the name of the YAML file that the YamlConfigBuilder reads from
     * @throws FileNotFoundException if the file can't be found
     */
    public YamlConfigBuilder(String YamlFileName) throws FileNotFoundException {
        this(new FileInputStream(YamlFileName));
        this.configurationFile = new File(YamlFileName);
    }

    /**
     * Constructs a YAMLConfigBuilder that reads from the given InputStream.
     *
     * @param inputStream the InputStream containing the YAML configuration
     * @throws IllegalArgumentException if inputStream is {@code null}
     */
    public YamlConfigBuilder(InputStream inputStream) {
        if (inputStream == null) {
            throw new IllegalArgumentException("inputStream can't be null");
        }
        this.in = inputStream;
    }

    /**
     * Constructs a YamlConfigBuilder that reads from the given URL.
     *
     * @param url the given url that the YamlConfigBuilder reads from
     * @throws IOException if URL is invalid
     */
    public YamlConfigBuilder(URL url) throws IOException {
        checkNotNull(url, "URL is null!");
        this.in = url.openStream();
        this.configurationUrl = url;
    }

    /**
     * Constructs a YamlConfigBuilder that tries to find a usable YAML configuration file.
     */
    public YamlConfigBuilder() {
        YamlConfigLocator locator = new YamlConfigLocator();
        this.in = locator.getIn();
        this.configurationFile = locator.getConfigurationFile();
        this.configurationUrl = locator.getConfigurationUrl();
    }

    @Override
    public Config build() {
        return build(new Config());
    }

    Config build(Config config) {
        config.setConfigurationFile(configurationFile);
        config.setConfigurationUrl(configurationUrl);
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return config;
    }

    private void parseAndBuildConfig(Config config) throws Exception {
        Node w3cRootNode;
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = ((YamlMapping) YamlLoader.load(in));
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        }

        YamlNode imdgRoot = yamlRootNode.childAsMapping(ConfigSections.HAZELCAST.name().toLowerCase());
        if (imdgRoot == null) {
            throw new InvalidConfigurationException("No mapping with hazelcast key is found in the provided configuration");
        }

        if (shouldValidate()) {
            validateDom(imdgRoot);
        }

        w3cRootNode = W3cDomUtil.asW3cNode(imdgRoot);

        new YamlMemberDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    // TODO YAML schema validation instead
    private void validateDom(YamlNode imdgRoot) {
        if (imdgRoot instanceof YamlMapping) {
            YamlMapping imdgRootMapping = (YamlMapping) imdgRoot;

            validateNetwork(imdgRootMapping);
            validateLiteMember(imdgRootMapping);
            validateQuorum(imdgRootMapping);
        }
    }

    private void validateNetwork(YamlMapping imdgRootMapping) {
        YamlMapping networkRoot = imdgRootMapping.childAsMapping("network");
        if (networkRoot != null) {
            YamlMapping memberAddressProvider = networkRoot.childAsMapping("member-address-provider");
            if (memberAddressProvider != null) {
                YamlScalar className = memberAddressProvider.childAsScalar("class-name");
                if (className == null) {
                    throw new InvalidConfigurationException("The class-name attribute of member-address-provider configuration "
                            + "is mandatory");
                }
            }
        }
    }

    private void validateLiteMember(YamlMapping imdgRootMapping) {
        YamlMapping liteMemberRoot = imdgRootMapping.childAsMapping("lite-member");
        if (liteMemberRoot != null) {
            YamlNode enabledNode = liteMemberRoot.child("enabled");
            if (enabledNode == null) {
                throw new InvalidConfigurationException("Enabled attribute of lite-member configuration is mandatory");
            }

            if (!(enabledNode instanceof YamlScalar)) {
                throw new InvalidConfigurationException("Enabled attribute of lite-member should be defined as a boolean scalar. "
                        + "It is defined as " + enabledNode.getClass().getName());
            }

            YamlScalar enabledScalarNode = (YamlScalar) enabledNode;
            if (!enabledScalarNode.isA(Boolean.class)) {
                throw new InvalidConfigurationException("Enabled attribute of lite-member should be defined as a boolean scalar. "
                        + "It is defined as " + enabledNode.getClass().getName());
            }
        }
    }

    private void validateQuorum(YamlMapping imdgRootMapping) {
        YamlMapping quorumRoot = imdgRootMapping.childAsMapping("quorum");
        if (quorumRoot != null) {
            for (YamlNode quorumNode : quorumRoot.children()) {
                if (quorumNode instanceof YamlMapping) {
                    YamlMapping quorumAttributes = (YamlMapping) quorumNode;
                    YamlNode memberCountQuorum = quorumAttributes.child("member-count-quorum");
                    YamlNode recentlyActiveQuorum = quorumAttributes.child("recently-active-quorum");
                    YamlNode probabilisticQuorum = quorumAttributes.child("probabilistic-quorum");

                    int quorumsDefined = 0;
                    if (memberCountQuorum != null) {
                        quorumsDefined++;
                    }

                    if (recentlyActiveQuorum != null) {
                        quorumsDefined++;
                    }

                    if (probabilisticQuorum != null) {
                        quorumsDefined++;
                    }

                    if (quorumsDefined > 1) {
                        throw new InvalidConfigurationException("A quorum cannot simultaneously define probabilistic-quorum or "
                                + "recently-active-quorum and a quorum function class name.");
                    }

                }
            }
        }
    }

    private boolean shouldValidate() {
        // in case of overridden Hazelcast version there may be no schema with that version
        // (this feature is used only in Simulator testing)
        return System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION) == null;
    }
}
