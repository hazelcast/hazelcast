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

import com.hazelcast.config.HashIndexConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.SortedIndexAttribute;
import com.hazelcast.config.SortedIndexConfig;

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

    /** Pattern to stipe away "this." prefix. */
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
        assert config instanceof SortedIndexConfig || config instanceof HashIndexConfig;

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

        IndexConfig res;

        if (config instanceof SortedIndexConfig) {
            StringBuilder nameBuilder = name != null ? new StringBuilder(mapName + "_sorted") : null;

            List<SortedIndexAttribute> originalAttributes = ((SortedIndexConfig)config).getAttributes();
            List<SortedIndexAttribute> newAttributes = new ArrayList<>(originalAttributes.size());

            for (int i = 0; i < originalAttributes.size(); i++) {
                String attributeName = normalizedAttributeNames.get(i);
                boolean attributeAsc = originalAttributes.get(i).isAsc();

                SortedIndexAttribute newAttribute = new SortedIndexAttribute(attributeName, attributeAsc);

                newAttributes.add(newAttribute);

                if (nameBuilder != null)
                    nameBuilder.append("_").append(attributeName).append("_").append(attributeAsc ? "asc" : "desc");
            }

            if (nameBuilder != null)
                name = nameBuilder.toString();

            res = new SortedIndexConfig().setName(name).setAttributes(newAttributes);
        }
        else {
            StringBuilder nameBuilder = name != null ? new StringBuilder(mapName + "_hash") : null;

            List<String> originalAttributes = ((HashIndexConfig)config).getAttributes();
            List<String> newAttributes = new ArrayList<>(originalAttributes.size());

            for (int i = 0; i < originalAttributes.size(); i++) {
                String attribute = normalizedAttributeNames.get(i);

                newAttributes.add(attribute);

                if (nameBuilder != null)
                    nameBuilder.append("_").append(attribute);
            }

            if (nameBuilder != null)
                name = nameBuilder.toString();

            res = new HashIndexConfig().setName(name).setAttributes(newAttributes);
        }

        return res;
    }

    /**
     * Get attribute names of the given index.
     *
     * @param config Index config.
     * @return Attribute names.
     */
    private static List<String> getAttributeNames(IndexConfig config) {
        List<String> res = null;

        if (config instanceof SortedIndexConfig) {
            List<SortedIndexAttribute> attributes = ((SortedIndexConfig)config).getAttributes();

            if (attributes != null && !attributes.isEmpty()) {
                res = new ArrayList<>(attributes.size());

                for (SortedIndexAttribute attribute : attributes)
                    res.add(attribute.getName());
            }
        }
        else {
            assert config instanceof HashIndexConfig;

            res = ((HashIndexConfig) config).getAttributes();
        }

        if (res == null)
            res = Collections.emptyList();

        return res;
    }

    public static List<IndexComponent> getComponents(IndexConfig config) {
        assert config != null;
        assert config instanceof SortedIndexConfig || config instanceof HashIndexConfig;

        List<IndexComponent> res;

        if (config instanceof SortedIndexConfig) {
            SortedIndexConfig config0 = (SortedIndexConfig)config;

            res = new ArrayList<>(config0.getAttributes().size());

            for (SortedIndexAttribute attribute : config0.getAttributes())
                res.add(new IndexComponent(attribute.getName(), attribute.isAsc()));
        }
        else {
            HashIndexConfig config0 = (HashIndexConfig)config;

            res = new ArrayList<>(config0.getAttributes().size());

            for (String attribute : config0.getAttributes())
                res.add(new IndexComponent(attribute, null));
        }

        return res;
    }

    public static String toString(List<IndexComponent> components) {
        if (components.size() == 0)
            return "[]";

        StringBuilder res = new StringBuilder("[");

        boolean first = true;

        for (IndexComponent component : components) {
            if (first)
                first = false;
            else
                res.append(", ");

            res.append(component);
        }

        res.append("]");

        return res.toString();
    }

    /**
     * Create simple index definition with only one attribute. For testing purposes only.
     *
     * @param ordered Whether the index should be ordered.
     * @param attributes Attribute names.
     * @return Index definition.
     */
    public static IndexConfig createSimpleIndexConfig(boolean ordered, String... attributes) {
        IndexConfig res;

        if (ordered) {
            SortedIndexConfig res0 = new SortedIndexConfig();

            for (String attribute : attributes)
                res0.addAttribute(new SortedIndexAttribute(attribute));

            res = res0;
        }
        else {
            HashIndexConfig res0 = new HashIndexConfig();

            for (String attribute : attributes)
                res0.addAttribute(attribute);

            res = res0;
        }

        return validateAndNormalize(UUID.randomUUID().toString(), res);
    }

    private IndexUtils() {
        // No-op.
    }
}
