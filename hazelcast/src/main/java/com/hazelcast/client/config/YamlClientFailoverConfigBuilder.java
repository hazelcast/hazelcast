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

package com.hazelcast.client.config;

import com.hazelcast.client.config.impl.ClientFailoverConfigSections;
import com.hazelcast.client.config.impl.YamlClientFailoverConfigLocator;
import com.hazelcast.client.config.impl.YamlClientFailoverDomConfigProcessor;
import com.hazelcast.config.AbstractYamlConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.config.ConfigLoader;
import com.hazelcast.internal.config.YamlConfigSchemaValidator;
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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Loads the {@link com.hazelcast.client.config.ClientFailoverConfig} using YAML.
 */
public class YamlClientFailoverConfigBuilder
        extends AbstractYamlConfigBuilder {

    private final InputStream in;

    public YamlClientFailoverConfigBuilder(String resource)
            throws IOException {
        URL url = ConfigLoader.locateConfig(resource);
        checkTrue(url != null, "Could not load " + resource);

        this.in = url.openStream();
    }

    public YamlClientFailoverConfigBuilder(File file)
            throws IOException {
        checkNotNull(file, "File is null!");
        this.in = new FileInputStream(file);
    }

    public YamlClientFailoverConfigBuilder(URL url)
            throws IOException {
        checkNotNull(url, "URL is null!");
        this.in = url.openStream();
    }

    public YamlClientFailoverConfigBuilder(InputStream in) {
        this.in = in;
    }

    /**
     * Loads the client failover config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.client.failover.config' is set. If it exist and
     * it begins with 'classpath:', then a classpath resource is loaded. Else it will assume it is a file
     * reference. The configuration file or resource will be loaded only if the postfix of its name ends
     * with '.yaml'.</li>
     * <li>it checks if a hazelcast-client-failover.yaml is available in the working dir</li>
     * <li>it checks if a hazelcast-client-failover.yaml is available on the classpath</li>
     * </ol>
     *
     * @throws HazelcastException if no failover configuration is found
     */
    public YamlClientFailoverConfigBuilder() {
        this((YamlClientFailoverConfigLocator) null);
    }

    /**
     * Constructs a {@link YamlClientFailoverConfigBuilder} that loads the configuration
     * with the provided {@link YamlClientFailoverConfigLocator}.
     * <p>
     * If the provided {@link YamlClientFailoverConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link YamlClientFailoverConfigLocator}.
     * <p>
     * If the provided {@link YamlClientFailoverConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration YAML to load
     * from. No further attempt to locate the configuration YAML is made
     * if the configuration YAML is not located already.
     *
     * @param locator the configured locator to use
     * @throws HazelcastException if no failover configuration is found
     */
    @PrivateApi
    public YamlClientFailoverConfigBuilder(YamlClientFailoverConfigLocator locator) {
        if (locator == null) {
            locator = new YamlClientFailoverConfigLocator();
            locator.locateEverywhere();
        }

        if (!locator.isConfigPresent()) {
            throw new HazelcastException("Failed to load ClientFailoverConfig");
        }

        this.in = locator.getIn();
    }

    public ClientFailoverConfig build() {
        ClientFailoverConfig clientFailoverConfig = new ClientFailoverConfig();
        build(clientFailoverConfig);
        return clientFailoverConfig;
    }

    public YamlClientFailoverConfigBuilder setProperties(Properties properties) {
        setPropertiesInternal(properties);
        return this;
    }

    void build(ClientFailoverConfig clientFailoverConfig) {
        try {
            parseAndBuildConfig(clientFailoverConfig);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void parseAndBuildConfig(ClientFailoverConfig config)
            throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = ((YamlMapping) YamlLoader.load(in));
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        }

        String configRoot = getConfigRoot();
        YamlNode clientFailoverRoot = yamlRootNode.childAsMapping(configRoot);
        if (clientFailoverRoot == null) {
            clientFailoverRoot = yamlRootNode;
        }

        YamlDomChecker.check(clientFailoverRoot, Collections.singleton(ClientFailoverConfigSections.CLIENT_FAILOVER.getName()));

        Node w3cRootNode = asW3cNode(clientFailoverRoot);
        replaceVariables(w3cRootNode);
        importDocuments(clientFailoverRoot);

        if (shouldValidateTheSchema()) {
            new YamlConfigSchemaValidator().validate(yamlRootNode);
        }

        new YamlClientFailoverDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    @Override
    protected String getConfigRoot() {
        return ClientFailoverConfigSections.CLIENT_FAILOVER.getName();
    }
}
