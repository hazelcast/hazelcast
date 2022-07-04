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

import com.hazelcast.config.replacer.PropertyReplacer;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.internal.config.ConfigLoader;
import com.hazelcast.internal.config.ConfigReplacerHelper;
import com.hazelcast.internal.config.ConfigSections;
import com.hazelcast.internal.config.YamlDomVariableReplacer;
import com.hazelcast.internal.config.yaml.YamlElementAdapter;
import com.hazelcast.internal.yaml.MutableYamlMapping;
import com.hazelcast.internal.yaml.MutableYamlSequence;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNameNodePair;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getAttribute;
import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.yaml.YamlUtil.asMapping;
import static com.hazelcast.internal.yaml.YamlUtil.asScalar;
import static com.hazelcast.internal.yaml.YamlUtil.asSequence;
import static com.hazelcast.internal.yaml.YamlUtil.isMapping;
import static com.hazelcast.internal.yaml.YamlUtil.isOfSameType;
import static com.hazelcast.internal.yaml.YamlUtil.isScalar;
import static com.hazelcast.internal.yaml.YamlUtil.isSequence;

/**
 * Contains logic for replacing system variables in the YAML file and importing YAML files from different locations.
 */
public abstract class AbstractYamlConfigBuilder extends AbstractConfigBuilder {
    private final Set<String> currentlyImportedFiles = new HashSet<>();
    private Properties properties = System.getProperties();

    /**
     * Gets the current used properties. Can be null if no properties are set.
     *
     * @return the current used properties
     * @see #setPropertiesInternal(Properties)
     */
    protected Properties getProperties() {
        return properties;
    }

    /**
     * Imports external YAML documents into the provided main YAML document.
     * <p>
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
    protected void importDocuments(YamlNode imdgRoot) throws Exception {
        YamlMapping rootAsMapping = asMapping(imdgRoot);
        YamlSequence importSeq = rootAsMapping.childAsSequence(ConfigSections.IMPORT.getName());
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
                throw new InvalidConfigurationException("Cyclic loading of resource '" + url.getPath() + "' detected!");
            }

            YamlNode rootLoaded;
            try (InputStream inputStream = url.openStream()) {
                rootLoaded = YamlLoader.load(inputStream);
            } catch (Exception ex) {
                throw new InvalidConfigurationException("Loading YAML document from resource " + url.getPath() + " failed", ex);
            }

            YamlNode imdgRootLoaded = asMapping(rootLoaded).child(getConfigRoot());
            if (imdgRootLoaded == null) {
                imdgRootLoaded = rootLoaded;
            }

            replaceVariables(asW3cNode(imdgRootLoaded));
            importDocuments(imdgRootLoaded);

            // we need to merge and not just substitute with the content of the imported document
            // YAML documents define mappings where the name of the nodes should be unique
            merge(imdgRootLoaded, imdgRoot);
        }
        replaceVariables(asW3cNode(imdgRoot));

        ((MutableYamlMapping) rootAsMapping).removeChild(ConfigSections.IMPORT.getName());
    }

    protected abstract String getConfigRoot();

    /**
     * Merges the source YAML document into the target YAML document
     * <p>
     * If a given source node is not found in the target, it will be attached
     * If a given source node is found in the target, this method is invoked
     * recursively with the given node
     *
     * @param source The source YAML document's root
     * @param target The target YAML document's root
     */
    private void merge(YamlNode source, YamlNode target) {
        if (source == null) {
            return;
        }

        checkAmbiguousConfiguration(source, target);

        if (isMapping(source)) {
            mergeMappingNodes(asMapping(source), asMapping(target));
        } else if (isSequence(source)) {
            mergeSequenceNodes(asSequence(source), asSequence(target));
        }
    }

    private void checkAmbiguousConfiguration(YamlNode source, YamlNode target) {
        if (!isOfSameType(source, target)) {
            String message = String.format("Ambiguous configuration of '%s': node types differ in the already loaded and imported"
                            + " configuration. Type of already loaded node: %s, type of imported node: %s",
                    target.path(), target.getClass().getSimpleName(), source.getClass().getSimpleName());
            throw new InvalidConfigurationException(message);
        }

        if (isScalar(source) && isScalar(target)) {
            Object sourceValue = ((YamlScalar) source).nodeValue();
            Object targetValue = ((YamlScalar) target).nodeValue();
            if (!targetValue.equals(sourceValue)) {
                throw new InvalidConfigurationException(
                        String.format("Ambiguous configuration of '%s': current and imported values "
                                + "differ. Current value: %s, imported value: %s", target.path(), targetValue, sourceValue));
            }
        }
    }

    private void mergeSequenceNodes(YamlSequence sourceAsSequence, YamlSequence targetAsSequence) {
        for (YamlNode sourceChild : sourceAsSequence.children()) {
            if (targetAsSequence instanceof MutableYamlSequence) {
                ((MutableYamlSequence) targetAsSequence).addChild(sourceChild);
            }
        }
    }

    private void mergeMappingNodes(YamlMapping sourceAsMapping, YamlMapping targetAsMapping) {
        for (YamlNode sourceChild : sourceAsMapping.children()) {
            YamlNode targetChild = targetAsMapping.child(sourceChild.nodeName());
            if (targetChild != null) {
                merge(sourceChild, targetChild);
            } else {
                if (targetAsMapping instanceof MutableYamlMapping) {
                    ((MutableYamlMapping) targetAsMapping).addChild(sourceChild.nodeName(), sourceChild);
                }
            }
        }
    }

    protected void replaceVariables(Node node) throws Exception {
        // if no config-replacer is defined, use backward compatible default behavior for missing properties
        boolean failFast = false;

        List<ConfigReplacer> replacers = new ArrayList<>();

        // Always use the Property replacer first.
        PropertyReplacer propertyReplacer = new PropertyReplacer();
        propertyReplacer.init(properties);
        replacers.add(propertyReplacer);

        // Add other replacers
        Node replacersNode = node.getAttributes().getNamedItem(ConfigSections.CONFIG_REPLACERS.getName());

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

    protected void setPropertiesInternal(Properties properties) {
        this.properties = properties;
    }

    private void fillReplacerProperties(Node node, Properties properties) {
        YamlMapping propertiesMapping = asMapping(((YamlElementAdapter) node).getYamlNode());
        for (YamlNameNodePair childNodePair : propertiesMapping.childrenPairs()) {
            String childName = childNodePair.nodeName();
            YamlNode child = childNodePair.childNode();
            Object nodeValue = asScalar(child).nodeValue();
            properties.put(childName, nodeValue != null ? nodeValue.toString() : "");
        }
    }
}
