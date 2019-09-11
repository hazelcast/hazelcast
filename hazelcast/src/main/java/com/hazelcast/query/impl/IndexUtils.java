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
    /** Maximum number of columns allowed in the index. */
    private static final int MAX_COLUMNS = 255;

    /** Pattern to stripe away "this." prefix. */
    private static final Pattern THIS_PATTERN = Pattern.compile("^this\\.");

    private IndexUtils() {
        // No-op.
    }

    /**
     * Validate provided index config and normalize it's name and column names.
     *
     * @param mapName Name of the map
     * @param config Index config.
     * @return Normalized index config.
     * @throws IllegalArgumentException If index configuration is invalid.
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    public static IndexConfig validateAndNormalize(String mapName, IndexConfig config) {
        assert config != null;

        // Validate columns.
        List<String> originalColumnNames = getColumnNames(config);

        if (originalColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Index must have at least one column: " + config);
        }

        if (originalColumnNames.size() > MAX_COLUMNS) {
            throw new IllegalArgumentException("Index cannot have more than " + MAX_COLUMNS
                + " columns: " + config);
        }

        List<String> normalizedColumnNames = new ArrayList<>(originalColumnNames.size());

        for (String originalColumnName : originalColumnNames) {
            validateColumn(config, originalColumnName);

            originalColumnName = originalColumnName.trim();

            String normalizedColumnName = canonicalizeAttribute(originalColumnName);

            assert !normalizedColumnName.isEmpty();

            int existingIdx = normalizedColumnNames.indexOf(normalizedColumnName);

            if (existingIdx != -1) {
                String duplicateOriginalColumnName = originalColumnNames.get(existingIdx);

                if (duplicateOriginalColumnName.equals(originalColumnName)) {
                    throw new IllegalArgumentException("Duplicate column name [config=" + config
                        + ", column=" + originalColumnName + ']');
                } else {
                    throw new IllegalArgumentException("Duplicate column names [config=" + config
                        + ", column1=" + duplicateOriginalColumnName + ", column2=" + originalColumnName
                        + ']');
                }
            }

            normalizedColumnNames.add(normalizedColumnName);
        }

        // Construct final index.
        String name = config.getName();

        if (name != null && name.trim().isEmpty()) {
            name = null;
        }

        return buildNormalizedConfig(mapName, config.getType(), name, normalizedColumnNames);
    }

    private static IndexConfig buildNormalizedConfig(String mapName, IndexType indexType, String indexName,
        List<String> normalizedColumnNames) {
        IndexConfig newConfig = new IndexConfig().setType(indexType);

        StringBuilder nameBuilder = indexName == null
            ? new StringBuilder(mapName + (indexType == IndexType.SORTED ? "_sorted" : "_hash")) : null;

        for (String newColumnName : normalizedColumnNames) {
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
     * Validate column name.
     *
     * @param config Index config.
     * @param columnName Column name.
     */
    public static void validateColumn(IndexConfig config, String columnName) {
        if (columnName == null) {
            throw new NullPointerException("Column name cannot be null: " + config);
        }

        String columnName0 = columnName.trim();

        if (columnName0.isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be empty: " + config);
        }

        if (columnName0.endsWith(".")) {
            throw new IllegalArgumentException("Column name cannot end with dot [config=" + config
                + ", column=" + columnName + ']');
        }
    }

    /**
     * Validate column name.
     *
     * @param columnName Column name.
     */
    public static void validateColumn(String columnName) {
        if (columnName == null) {
            throw new NullPointerException("Column name cannot be null.");
        }

        String columnName0 = columnName.trim();

        if (columnName0.isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be empty.");
        }

        if (columnName0.endsWith(".")) {
            throw new IllegalArgumentException("Column name cannot end with dot: " + columnName + ']');
        }
    }

    /**
     * Get column names of the given index.
     *
     * @param config Index config.
     * @return Column names.
     */
    private static List<String> getColumnNames(IndexConfig config) {
        if (config.getColumns().isEmpty()) {
            return Collections.emptyList();
        }

        List<String> res = new ArrayList<>(config.getColumns().size());

        for (IndexColumnConfig column: config.getColumns()) {
            res.add(column.getName());
        }

        return res;
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

    public static List<String> getComponents(IndexConfig config) {
        assert config != null;

        List<String> res = new ArrayList<>(config.getColumns().size());

        for (IndexColumnConfig column : config.getColumns()) {
            res.add(column.getName());
        }

        return res;
    }

    /**
     * Create simple index definition with the given columns. For testing purposes only.
     *
     * @param ordered Whether the index should be ordered.
     * @param columns Column names.
     * @return Index definition.
     */
    public static IndexConfig createSimpleIndexConfig(boolean ordered, String... columns) {
        IndexConfig res = new IndexConfig();

        res.setType(ordered ? IndexType.SORTED : IndexType.HASH);

        for (String column : columns) {
            res.addColumn(column);
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
