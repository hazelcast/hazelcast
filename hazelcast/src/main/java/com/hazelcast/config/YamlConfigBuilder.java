/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.ConfigSections;
import com.hazelcast.internal.config.YamlConfigLocator;
import com.hazelcast.internal.config.YamlConfigSchemaValidator;
import com.hazelcast.internal.config.YamlMemberDomConfigProcessor;
import com.hazelcast.internal.config.yaml.YamlDomChecker;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.spi.annotation.PrivateApi;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * A YAML {@link ConfigBuilder} implementation.
 * <p>
 * This config builder is compatible with the YAML 1.2 specification and
 * supports the JSON Schema.
 * <p>
 * Unlike {@link Config#load()} and its variants, a configuration constructed via
 * {@code YamlConfigBuilder} does not apply overrides found in environment variables/system properties.
 */
public class YamlConfigBuilder extends AbstractYamlConfigBuilder implements ConfigBuilder {

    private final InputStream in;

    private File configurationFile;
    private URL configurationUrl;

    /**
     * Constructs a YamlConfigBuilder that reads from the provided YAML file.
     *
     * @param yamlFileName the name of the YAML file that the YamlConfigBuilder reads from
     * @throws FileNotFoundException if the file can't be found
     */
    public YamlConfigBuilder(String yamlFileName) throws FileNotFoundException {
        this(new FileInputStream(yamlFileName));
        this.configurationFile = new File(yamlFileName);
    }

    /**
     * Constructs a YAMLConfigBuilder that reads from the given InputStream.
     *
     * @param inputStream the InputStream containing the YAML configuration
     * @throws IllegalArgumentException if inputStream is {@code null}
     */
    public YamlConfigBuilder(InputStream inputStream) {
        checkTrue(inputStream != null, "inputStream can't be null");
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
        this((YamlConfigLocator) null);
    }

    /**
     * Constructs a {@link YamlConfigBuilder} that loads the configuration
     * with the provided {@link YamlConfigLocator}.
     * <p>
     * If the provided {@link YamlConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link YamlConfigLocator}.
     * <p>
     * If the provided {@link YamlConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration YAML to load
     * from. No further attempt to locate the configuration YAML is made
     * if the configuration YAML is not located already.
     *
     * @param locator the configured locator to use
     */
    @PrivateApi
    public YamlConfigBuilder(YamlConfigLocator locator) {
        if (locator == null) {
            locator = new YamlConfigLocator();
            locator.locateEverywhere();
        }

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
        } finally {
            IOUtil.closeResource(in);
        }
        return config;
    }

    private void parseAndBuildConfig(Config config) throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = ((YamlMapping) YamlLoader.load(in));
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        }

        YamlNode root = yamlRootNode.childAsMapping(ConfigSections.HAZELCAST.getName());
        if (root == null) {
            root = yamlRootNode;
        }

        YamlDomChecker.check(root, Collections.singleton(ConfigSections.HAZELCAST.getName()));

        Node w3cRootNode = asW3cNode(root);
        replaceVariables(w3cRootNode);
        importDocuments(root);

        if (shouldValidateTheSchema()) {
            new YamlConfigSchemaValidator().validate(yamlRootNode);
        }

        new YamlMemberDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    public YamlConfigBuilder setProperties(Properties properties) {
        setPropertiesInternal(properties);
        return this;
    }

    @Override
    protected String getConfigRoot() {
        return ConfigSections.HAZELCAST.getName();
    }
}
