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

package com.hazelcast.query.impl;

import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.DomConfigHelper;
import com.hazelcast.config.IndexColumnConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.util.UuidUtil;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;

/**
 * Utility methods for indexes.
 */
public final class IndexUtils {
    /** Maximum number of attributes allowed in the index. */
    private static final int MAX_ATTRIBUTES = 255;

    /** Pattern to stripe away "this." prefix. */
    private static final Pattern THIS_PATTERN = Pattern.compile("^this\\.");

    private IndexUtils() {
        // No-op.
    }

    /**
     * Validate provided index config and normalize it's name and attribute names.
     *
     * @param mapName Name of the map
     * @param config Index config.
     * @return Normalized index config.
     * @throws IllegalArgumentException If index configuration is invalid.
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    public static IndexConfig validateAndNormalize(String mapName, IndexConfig config) {
        assert config != null;

        // Validate attributes.
        List<String> originalAttributeNames = getAttributeNames(config);

        if (originalAttributeNames.isEmpty()) {
            throw new IllegalArgumentException("Index must have at least one attribute: " + config);
        }

        if (originalAttributeNames.size() > MAX_ATTRIBUTES) {
            throw new IllegalArgumentException("Index cannot have more than " + MAX_ATTRIBUTES
                + " attributes: " + config);
        }

        List<String> normalizedAttributeNames = new ArrayList<>(originalAttributeNames.size());

        for (String originalAttributeName : originalAttributeNames) {
            if (originalAttributeName == null) {
                throw new IllegalArgumentException("Attribute name cannot be null: " + config);
            }

            originalAttributeName = originalAttributeName.trim();

            if (originalAttributeName.isEmpty()) {
                throw new IllegalArgumentException("Attribute name cannot be empty: " + config);
            }

            if (originalAttributeName.endsWith(".")) {
                throw new IllegalArgumentException("Invalid attribute name [config=" + config
                    + ", attribute=" + originalAttributeName + ']');
            }

            String normalizedAttributeName = canonicalizeAttribute(originalAttributeName);

            assert !normalizedAttributeName.isEmpty();

            int existingIdx = normalizedAttributeNames.indexOf(normalizedAttributeName);

            if (existingIdx != -1) {
                String duplicateOriginalAttributeName = originalAttributeNames.get(existingIdx);

                if (duplicateOriginalAttributeName.equals(originalAttributeName)) {
                    throw new IllegalArgumentException("Duplicate attribute name [config=" + config
                        + ", attribute=" + originalAttributeName + ']');
                } else {
                    throw new IllegalArgumentException("Duplicated attribute names [config=" + config
                        + ", attribute1=" + duplicateOriginalAttributeName + ", attribute2=" + originalAttributeName
                        + ']');
                }
            }

            normalizedAttributeNames.add(normalizedAttributeName);
        }

        // Construct final index.
        String name = config.getName();

        if (name != null && name.trim().isEmpty()) {
            name = null;
        }

        return buildNormalizedConfig(mapName, config.getType(), name, normalizedAttributeNames);
    }

    private static IndexConfig buildNormalizedConfig(String mapName, IndexType indexType, String indexName,
        List<String> normalizedAttributeNames) {
        IndexConfig newConfig = new IndexConfig().setType(indexType);

        StringBuilder nameBuilder = indexName == null
            ? new StringBuilder(mapName + (indexType == IndexType.SORTED ? "_sorted" : "_hash")) : null;

        for (String newColumnName : normalizedAttributeNames) {
            newConfig.addColumn(newColumnName);

            if (nameBuilder != null) {
                nameBuilder.append("_").append(newColumnName);
            }
        }

        if (nameBuilder != null) {
            indexName = nameBuilder.toString();
        }

        newConfig.setName(indexName);

        return newConfig;
    }

    /**
     * Produces canonical attribute representation by stripping an unnecessary
     * "this." qualifier from the passed attribute, if any.
     *
     * @param attribute the attribute to canonicalize.
     * @return the canonical attribute representation.
     */
    public static String canonicalizeAttribute(String attribute) {
        return THIS_PATTERN.matcher(attribute).replaceFirst("");
    }

    /**
     * Get attribute names of the given index.
     *
     * @param config Index config.
     * @return Attribute names.
     */
    private static List<String> getAttributeNames(IndexConfig config) {
        if (config.getColumns().isEmpty()) {
            return Collections.emptyList();
        }

        List<String> res = new ArrayList<>(config.getColumns().size());

        for (IndexColumnConfig column: config.getColumns()) {
            res.add(column.getName());
        }

        return res;
    }

    public static List<String> getComponents(IndexConfig config) {
        assert config != null;

        List<String> res = new ArrayList<>(config.getColumns().size());

        for (IndexColumnConfig column : config.getColumns()) {
            res.add(column.getName());
        }

        return res;
    }

    /**
     * Create simple index definition with only one attribute. For testing purposes only.
     *
     * @param ordered Whether the index should be ordered.
     * @param attributes Attribute names.
     * @return Index definition.
     */
    public static IndexConfig createSimpleIndexConfig(boolean ordered, String... attributes) {
        IndexConfig res = new IndexConfig();

        res.setType(ordered ? IndexType.SORTED : IndexType.HASH);

        for (String attribute : attributes) {
            res.addColumn(attribute);
        }

        return validateAndNormalize(UuidUtil.newUnsecureUUID().toString(), res);
    }

    public static void generateXml(ConfigXmlGenerator.XmlGenerator gen, List<IndexConfig> indexConfigs) {
        if (indexConfigs.isEmpty()) {
            return;
        }

        gen.open("indexes");
        for (IndexConfig indexCfg : indexConfigs) {
            if (indexCfg.getName() != null) {
                gen.open("index", "name", indexCfg.getName(), "type", indexCfg.getType().name());
            } else {
                gen.open("index", "type", indexCfg.getType().name());
            }

            gen.open("columns");

            for (IndexColumnConfig column : indexCfg.getColumns()) {
                gen.node("column", column.getName());
            }

            gen.close();
            gen.close();
        }
        gen.close();
    }

    public static IndexConfig getIndexConfigFromXml(Node indexNode, boolean domLevel3) {
        NamedNodeMap attrs = indexNode.getAttributes();

        String name = DomConfigHelper.getTextContent(attrs.getNamedItem("name"), domLevel3);

        if (name.isEmpty()) {
            name = null;
        }

        String typeStr = DomConfigHelper.getTextContent(attrs.getNamedItem("type"), domLevel3);

        IndexType type = getIndexTypeFromXmlName(typeStr);

        IndexConfig res = new IndexConfig().setName(name).setType(type);

        for (Node columnsNode : childElements(indexNode)) {
            if ("columns".equals(cleanNodeName(columnsNode))) {
                for (Node columnNode : childElements(columnsNode)) {
                    if ("column".equals(cleanNodeName(columnNode))) {
                        String column = DomConfigHelper.getTextContent(columnNode, domLevel3);

                        res.addColumn(column);
                    }
                }
            }
        }

        return res;
    }

    public static IndexType getIndexTypeFromXmlName(String typeStr) {
        if (typeStr == null || typeStr.isEmpty()) {
            typeStr = IndexConfig.DEFAULT_TYPE.name();
        }

        typeStr = typeStr.toLowerCase();

        if (typeStr.equals(IndexType.SORTED.name().toLowerCase())) {
            return IndexType.SORTED;
        } else if (typeStr.equals(IndexType.HASH.name().toLowerCase())) {
            return IndexType.HASH;
        } else {
            throw new IllegalArgumentException("Unsupported index type: " + typeStr);
        }
    }

    public static IndexConfig getIndexConfigFromYaml(Node indexNode, boolean domLevel3) {
        NamedNodeMap attrs = indexNode.getAttributes();

        String name = DomConfigHelper.getTextContent(attrs.getNamedItem("name"), domLevel3);

        if (name.isEmpty()) {
            name = null;
        }

        String typeStr = DomConfigHelper.getTextContent(attrs.getNamedItem("type"), domLevel3);

        if (typeStr.isEmpty()) {
            typeStr = IndexConfig.DEFAULT_TYPE.name();
        }

        typeStr = typeStr.toLowerCase();

        IndexType type;

        if (typeStr.equals(IndexType.SORTED.name().toLowerCase())) {
            type = IndexType.SORTED;
        } else if (typeStr.equals(IndexType.HASH.name().toLowerCase())) {
            type = IndexType.HASH;
        } else {
            throw new IllegalArgumentException("Unsupported index type: " + typeStr);
        }

        IndexConfig res = new IndexConfig().setName(name).setType(type);

        Node columnsNode = attrs.getNamedItem("columns");

        for (Node columnNode : childElements(columnsNode)) {
            String column = columnNode.getNodeValue();

            res.addColumn(column);
        }

        return res;
    }
}
