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

import com.hazelcast.client.config.impl.ClientConfigSections;
import com.hazelcast.client.config.impl.YamlClientConfigLocator;
import com.hazelcast.client.config.impl.YamlClientDomConfigProcessor;
import com.hazelcast.config.AbstractYamlConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.YamlConfigSchemaValidator;
import com.hazelcast.internal.config.ConfigLoader;
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
 * Loads the {@link com.hazelcast.client.config.ClientConfig} using YAML.
 */
public class YamlClientConfigBuilder extends AbstractYamlConfigBuilder {

    private final InputStream in;

    public YamlClientConfigBuilder(String resource) throws IOException {
        URL url = ConfigLoader.locateConfig(resource);
        checkTrue(url != null, "Could not load " + resource);
        this.in = url.openStream();
    }

    public YamlClientConfigBuilder(File file) throws IOException {
        checkNotNull(file, "File is null!");
        this.in = new FileInputStream(file);
    }

    public YamlClientConfigBuilder(URL url) throws IOException {
        checkNotNull(url, "URL is null!");
        this.in = url.openStream();
    }

    public YamlClientConfigBuilder(InputStream in) {
        this.in = in;
    }

    /**
     * Loads the client config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.client.config' is set. If it exist and
     * it begins with 'classpath:', then a classpath resource is loaded. Else it will assume it is a file
     * reference. The configuration file or resource will be loaded only if the postfix of its name ends
     * with `.yaml`.</li>
     * <li>it checks if a hazelcast-client.yaml is available in the working dir</li>
     * <li>it checks if a hazelcast-client.yaml is available on the classpath</li>
     * <li>it loads the hazelcast-client-default.yaml</li>
     * </ol>
     */
    public YamlClientConfigBuilder() {
        this((YamlClientConfigLocator) null);
    }

    /**
     * Constructs a {@link YamlClientConfigBuilder} that loads the configuration
     * with the provided {@link YamlClientConfigLocator}.
     * <p>
     * If the provided {@link YamlClientConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link YamlClientConfigLocator}.
     * <p>
     * If the provided {@link YamlClientConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration YAML to load
     * from. No further attempt to locate the configuration YAML is made
     * if the configuration YAML is not located already.
     *
     * @param locator the configured locator to use
     */
    @PrivateApi
    public YamlClientConfigBuilder(YamlClientConfigLocator locator) {
        if (locator == null) {
            locator = new YamlClientConfigLocator();
            locator.locateEverywhere();
        }

        this.in = locator.getIn();
    }

    public ClientConfig build() {
        return build(Thread.currentThread().getContextClassLoader());
    }

    public ClientConfig build(ClassLoader classLoader) {
        ClientConfig clientConfig = new ClientConfig();
        build(clientConfig, classLoader);
        return clientConfig;
    }

    public YamlClientConfigBuilder setProperties(Properties properties) {
        setPropertiesInternal(properties);
        return this;
    }

    void build(ClientConfig clientConfig, ClassLoader classLoader) {
        clientConfig.setClassLoader(classLoader);
        try {
            parseAndBuildConfig(clientConfig);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void parseAndBuildConfig(ClientConfig config) throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = ((YamlMapping) YamlLoader.load(in));
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        }

        YamlNode clientRoot = yamlRootNode.childAsMapping(ClientConfigSections.HAZELCAST_CLIENT.getName());
        if (clientRoot == null) {
            clientRoot = yamlRootNode;
        }
        YamlDomChecker.check(clientRoot, Collections.singleton(ClientConfigSections.HAZELCAST_CLIENT.getName()));

        Node w3cRootNode = asW3cNode(clientRoot);
        replaceVariables(w3cRootNode);
        importDocuments(clientRoot);

        if (shouldValidateTheSchema()) {
            new YamlConfigSchemaValidator().validate(yamlRootNode);
        }

        new YamlClientDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    @Override
    protected String getConfigRoot() {
        return ClientConfigSections.HAZELCAST_CLIENT.getName();
    }
}
