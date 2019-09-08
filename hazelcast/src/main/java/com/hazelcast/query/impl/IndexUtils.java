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

import com.hazelcast.config.IndexColumn;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Utility methods for indexes.
 */
public class IndexUtils {
    /** Maximum number of attributes allowed in the index. */
    private static final int MAX_ATTRIBUTES = 255;

    /** Pattern to stripe away "this." prefix. */
    private static final Pattern THIS_PATTERN = Pattern.compile("^this\\.");

    /**
     * Validate provided index config and normalize it's name and attribute names.
     *
     * @param mapName Name of the map
     * @param config Index config.
     * @return Normalized index config.
     * @throws IllegalArgumentException If index configuration is invalid.
     */
    public static IndexConfig validateAndNormalize(String mapName, IndexConfig config) {
        assert config != null;

        // Validate attributes.
        List<String> originalAttributeNames = getAttributeNames(config);

        if (originalAttributeNames.isEmpty())
            throw new IllegalArgumentException("Index must have at least one attribute: " + config);

        if (originalAttributeNames.size() > MAX_ATTRIBUTES)
            throw new IllegalArgumentException("Index cannot have more than " + MAX_ATTRIBUTES +
                " attributes: " + config);

        List<String> normalizedAttributeNames = new ArrayList<>(originalAttributeNames.size());

        for (String originalAttributeName : originalAttributeNames) {
            if (originalAttributeName == null)
                throw new IllegalArgumentException("Attribute name cannot be null: " + config);

            originalAttributeName = originalAttributeName.trim();

            if (originalAttributeName.isEmpty())
                throw new IllegalArgumentException("Attribute name cannot be empty: " + config);

            if (originalAttributeName.endsWith(".")) {
                throw new IllegalArgumentException("Invalid attribute name [config=" + config +
                    ", attribute=" + originalAttributeName + ']');
            }

            String normalizedAttributeName = THIS_PATTERN.matcher(originalAttributeName).replaceFirst("");

            assert !normalizedAttributeName.isEmpty();

            int existingIdx = normalizedAttributeNames.indexOf(normalizedAttributeName);

            if (existingIdx != -1) {
                String duplicateOriginalAttributeName = originalAttributeNames.get(existingIdx);

                if (duplicateOriginalAttributeName.equals(originalAttributeName)) {
                    throw new IllegalArgumentException("Duplicate attribute name [config=" + config +
                        ", attribute=" + originalAttributeName + ']');
                }
                else {
                    throw new IllegalArgumentException("Duplicated attribute names [config=" + config +
                        ", attribute1=" + duplicateOriginalAttributeName +
                        ", attribute2=" + originalAttributeName + ']');
                }
            }

            normalizedAttributeNames.add(normalizedAttributeName);
        }

        // Construct final index.
        String name = config.getName();

        if (name != null && name.trim().isEmpty())
            name = null;

        IndexConfig newConfig = new IndexConfig().setType(config.getType());

        StringBuilder nameBuilder = name != null ?
            new StringBuilder(mapName + (config.getType() == IndexType.SORTED ? "_sorted" : "_hash")) : null;

        for (int i = 0; i < config.getColumns().size(); i++) {
            String newColumnName = normalizedAttributeNames.get(i);

            newConfig.addColumn(newColumnName);

            if (nameBuilder != null)
                nameBuilder.append("_").append(newColumnName);
        }

        if (nameBuilder != null)
            name = nameBuilder.toString();

        newConfig.setName(name);

        return newConfig;
    }

    /**
     * Get attribute names of the given index.
     *
     * @param config Index config.
     * @return Attribute names.
     */
    private static List<String> getAttributeNames(IndexConfig config) {
        if (config.getColumns().isEmpty())
            return Collections.emptyList();

        List<String> res = new ArrayList<>(config.getColumns().size());

        for (IndexColumn column: config.getColumns())
            res.add(column.getName());

        return res;
    }

    public static List<String> getComponents(IndexConfig config) {
        assert config != null;

        List<String> res = new ArrayList<>(config.getColumns().size());

        for (IndexColumn column : config.getColumns())
            res.add(column.getName());

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

        for (String attribute : attributes)
            res.addColumn(attribute);

        return validateAndNormalize(UUID.randomUUID().toString(), res);
    }

    private IndexUtils() {
        // No-op.
    }
}
