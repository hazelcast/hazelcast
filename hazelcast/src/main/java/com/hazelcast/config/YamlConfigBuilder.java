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

package com.hazelcast.config;

import com.hazelcast.config.replacer.PropertyReplacer;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.internal.yaml.MutableYamlMapping;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNameNodePair;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlSequence;
import com.hazelcast.util.ExceptionUtil;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.config.DomConfigHelper.getAttribute;
import static com.hazelcast.config.yaml.W3cDomUtil.asW3cNode;
import static com.hazelcast.config.yaml.W3cDomUtil.getWrappedYamlMapping;
import static com.hazelcast.internal.yaml.YamlUtil.asMapping;
import static com.hazelcast.internal.yaml.YamlUtil.asScalar;
import static com.hazelcast.internal.yaml.YamlUtil.ensureRunningOnJava8OrHigher;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * A YAML {@link ConfigBuilder} implementation.
 * <p/>
 * This config builder is compatible with the YAML 1.2 specification and
 * supports the JSON Schema.
 */
public class YamlConfigBuilder implements ConfigBuilder {

    private final Set<String> currentlyImportedFiles = new HashSet<String>();
    private final InputStream in;

    private File configurationFile;
    private URL configurationUrl;
    private Properties properties;

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
        this((YamlConfigLocator) null);
    }

    /**
     * Constructs a {@link YamlConfigBuilder} that loads the configuration
     * with the provided {@link YamlConfigLocator}.
     * <p/>
     * If the provided {@link YamlConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link YamlConfigLocator}.
     * <p/>
     * If the provided {@link YamlConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration YAML to load
     * from. No further attempt to locate the configuration YAML is made
     * if the configuration YAML is not located already.
     *
     * @param locator the configured locator to use
     */
    public YamlConfigBuilder(YamlConfigLocator locator) {
        if (locator == null) {
            locator = new YamlConfigLocator();
            locator.locateFromSystemProperty();
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
        ensureRunningOnJava8OrHigher();

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

        Node w3cRootNode = asW3cNode(imdgRoot);
        replaceVariables(w3cRootNode);
        importDocuments(imdgRoot);

        new YamlMemberDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }

    /**
     * Imports external YAML documents into the provided main YAML document.
     * <p/>
     * Since the YAML configuration uses mappings, in order to keep the
     * configuration defined in the main YAML document the imported
     * document (the source) will be actually merged into the main
     * document (the target). An example to it is defining one map in the
     * main document, and another map in the imported document. In this
     * case the documents should be merged to include both map configurations
     * under the {@code root/map} node.
     *
     * @param imdgRoot The root of the main YAML configuration document
     * @throws Exception If a YAML document to be imported can't be loaded
     * @see #merge(YamlNode, YamlNode)
     */
    private void importDocuments(YamlNode imdgRoot) throws Exception {
        YamlMapping rootAsMapping = asMapping(imdgRoot);
        YamlSequence importSeq = rootAsMapping.childAsSequence(ConfigSections.IMPORT.name);
        if (importSeq == null || importSeq.childCount() == 0) {
            return;
        }

        for (YamlNode importNode : importSeq.children()) {
            String resource = asScalar(importNode).nodeValue();
            URL url = ConfigLoader.locateConfig(resource);
            if (url == null) {
                throw new InvalidConfigurationException("Failed to load resource: " + resource);
            }
            if (!currentlyImportedFiles.add(url.getPath())) {
                throw new InvalidConfigurationException("Resource '" + url.getPath() + "' is already loaded! This can be due to"
                        + " duplicate or cyclic imports.");
            }

            YamlNode rootLoaded;
            try {
                rootLoaded = YamlLoader.load(url.openStream());
            } catch (Exception ex) {
                throw new InvalidConfigurationException("Loading YAML document from resource " + url.getPath() + " failed", ex);
            }

            YamlNode imdgRootLoaded = asMapping(rootLoaded).child(ConfigSections.HAZELCAST.name.toLowerCase());
            if (imdgRootLoaded == null) {
                return;
            }

            replaceVariables(asW3cNode(imdgRootLoaded));
            importDocuments(imdgRootLoaded);

            merge(imdgRootLoaded, imdgRoot);
        }

        ((MutableYamlMapping) rootAsMapping).removeChild(ConfigSections.IMPORT.name);
    }

    /**
     * Merges the source YAML document into the target YAML document.
     * <p/>
     * If a given source node is not found in the target, it will be attached.
     * If a given source node is found in the target, this method is invoked
     * recursively with the given node.
     *
     * @param source The source YAML document's root
     * @param target The target YAML document's root
     */
    private void merge(YamlNode source, YamlNode target) {
        YamlMapping sourceAsMapping = asMapping(source);
        YamlMapping targetAsMapping = asMapping(target);

        for (YamlNode sourceChild : sourceAsMapping.children()) {
            YamlNode targetChild = targetAsMapping.child(sourceChild.nodeName());
            if (targetChild != null) {
                merge(sourceChild, targetChild);
            } else {
                ((MutableYamlMapping) targetAsMapping).addChild(sourceChild.nodeName(), sourceChild);
            }
        }
    }

    private void replaceVariables(Node node) throws Exception {
        // if no config-replacer is defined, use backward compatible default behavior for missing properties
        boolean failFast = false;

        List<ConfigReplacer> replacers = new ArrayList<ConfigReplacer>();

        // Always use the Property replacer first.
        PropertyReplacer propertyReplacer = new PropertyReplacer();
        propertyReplacer.init(properties);
        replacers.add(propertyReplacer);

        // Add other replacers
        Node replacersNode = node.getAttributes().getNamedItem(ConfigSections.CONFIG_REPLACERS.name);

        if (replacersNode != null) {
            String failFastAttr = getAttribute(replacersNode, "fail-if-value-missing", true);
            failFast = isNullOrEmpty(failFastAttr) || Boolean.parseBoolean(failFastAttr);
            for (Node n : childElements(replacersNode)) {
                String nodeName = cleanNodeName(n);
                if ("replacers".equals(nodeName)) {
                    for (Node replacerNode : childElements(n)) {
                        replacers.add(createReplacer(replacerNode));
                    }
                }
            }
        }

        ConfigReplacerHelper.traverseChildrenAndReplaceVariables(node, replacers, failFast, new YamlDomVariableReplacer());
    }

    private ConfigReplacer createReplacer(Node node) throws Exception {
        String replacerClass = getAttribute(node, "class-name", true);
        Properties properties = new Properties();
        for (Node n : childElements(node)) {
            String value = cleanNodeName(n);
            if ("properties".equals(value)) {
                fillReplacerProperties(n, properties);
            }
        }
        ConfigReplacer replacer = (ConfigReplacer) Class.forName(replacerClass).newInstance();
        replacer.init(properties);
        return replacer;
    }

    public YamlConfigBuilder setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    private void fillReplacerProperties(Node node, Properties properties) {
        YamlMapping propertiesMapping = getWrappedYamlMapping(node);
        for (YamlNameNodePair childNodePair : propertiesMapping.childrenPairs()) {
            String childName = childNodePair.nodeName();
            YamlNode child = childNodePair.childNode();
            Object nodeValue = asScalar(child).nodeValue();
            properties.put(childName, nodeValue != null ? nodeValue.toString() : "");
        }
    }

}
