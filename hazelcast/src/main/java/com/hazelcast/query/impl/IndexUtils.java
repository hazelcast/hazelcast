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

package com.hazelcast.query.impl;

import com.hazelcast.config.BitmapIndexOptions;
import com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.internal.util.UuidUtil;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.internal.config.DomConfigHelper.childElementWithName;
import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Utility methods for indexes.
 */
public final class IndexUtils {
    /** Maximum number of attributes allowed in the index. */
    public static final int MAX_ATTRIBUTES = 255;

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
        List<String> originalAttributeNames = config.getAttributes();

        if (originalAttributeNames.isEmpty()) {
            throw new IllegalArgumentException("Index must have at least one attribute: " + config);
        }

        if (originalAttributeNames.size() > MAX_ATTRIBUTES) {
            throw new IllegalArgumentException("Index cannot have more than " + MAX_ATTRIBUTES
                + " attributes: " + config);
        }

        if (config.getType() == IndexType.BITMAP && originalAttributeNames.size() > 1) {
            throw new IllegalArgumentException("Composite bitmap indexes are not supported: " + config);
        }

        List<String> normalizedAttributeNames = new ArrayList<>(originalAttributeNames.size());

        for (String originalAttributeName : originalAttributeNames) {
            validateAttribute(config, originalAttributeName);

            originalAttributeName = originalAttributeName.trim();

            String normalizedAttributeName = canonicalizeAttribute(originalAttributeName);

            assert !normalizedAttributeName.isEmpty();

            int existingIdx = normalizedAttributeNames.indexOf(normalizedAttributeName);

            if (existingIdx != -1) {
                String duplicateOriginalAttributeName = originalAttributeNames.get(existingIdx);

                if (duplicateOriginalAttributeName.equals(originalAttributeName)) {
                    throw new IllegalArgumentException("Duplicate attribute name [attributeName="
                        + originalAttributeName + ", indexConfig=" + config + ']');
                } else {
                    throw new IllegalArgumentException("Duplicate attribute names ["
                        + "attributeName1=" + duplicateOriginalAttributeName + ", attributeName2="
                        + originalAttributeName + ", indexConfig=" + config + ']');
                }
            }

            normalizedAttributeNames.add(normalizedAttributeName);
        }

        // Construct final index.
        String name = config.getName();

        if (name != null && name.trim().isEmpty()) {
            name = null;
        }

        IndexConfig normalizedConfig = buildNormalizedConfig(mapName, config.getType(), name, normalizedAttributeNames);

        if (config.getType() == IndexType.BITMAP) {
            String uniqueKey = config.getBitmapIndexOptions().getUniqueKey();
            UniqueKeyTransformation uniqueKeyTransformation = config.getBitmapIndexOptions().getUniqueKeyTransformation();

            validateAttribute(uniqueKey);
            uniqueKey = canonicalizeAttribute(uniqueKey);

            normalizedConfig.getBitmapIndexOptions().setUniqueKey(uniqueKey).setUniqueKeyTransformation(uniqueKeyTransformation);
        }

        return normalizedConfig;
    }

    private static IndexConfig buildNormalizedConfig(String mapName, IndexType indexType, String indexName,
                                                     List<String> normalizedAttributeNames) {
        IndexConfig newConfig = new IndexConfig().setType(indexType);

        StringBuilder nameBuilder = indexName == null
            ? new StringBuilder(mapName + "_" + getIndexTypeName(indexType)) : null;

        for (String normalizedAttributeName : normalizedAttributeNames) {
            newConfig.addAttribute(normalizedAttributeName);

            if (nameBuilder != null) {
                nameBuilder.append("_").append(normalizedAttributeName);
            }
        }

        if (nameBuilder != null) {
            indexName = nameBuilder.toString();
        }

        newConfig.setName(indexName);

        return newConfig;
    }

    /**
     * Validate attribute name.
     *
     * @param config Index config.
     * @param attributeName Attribute name.
     */
    public static void validateAttribute(IndexConfig config, String attributeName) {
        if (attributeName == null) {
            throw new NullPointerException("Attribute name cannot be null: " + config);
        }

        String attributeName0 = attributeName.trim();

        if (attributeName0.isEmpty()) {
            throw new IllegalArgumentException("Attribute name cannot be empty: " + config);
        }

        if (attributeName0.endsWith(".")) {
            throw new IllegalArgumentException("Attribute name cannot end with dot [config=" + config
                + ", attribute=" + attributeName + ']');
        }
    }

    /**
     * Validate attribute name.
     *
     * @param attributeName Attribute name.
     */
    public static void validateAttribute(String attributeName) {
        if (attributeName == null) {
            throw new NullPointerException("Attribute name cannot be null.");
        }

        String attributeName0 = attributeName.trim();

        if (attributeName0.isEmpty()) {
            throw new IllegalArgumentException("Attribute name cannot be empty.");
        }

        if (attributeName0.endsWith(".")) {
            throw new IllegalArgumentException("Attribute name cannot end with dot: " + attributeName);
        }
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

    public static String[] getComponents(IndexConfig config) {
        assert config != null;

        List<String> attributes = config.getAttributes();

        String[] res = new String[attributes.size()];

        for (int i = 0; i < attributes.size(); i++) {
            res[i] = attributes.get(i);
        }

        return res;
    }

    /**
     * Create simple index definition with the given attributes
     *
     * @param type Index type.
     * @param attributes Attribute names.
     * @return Index definition.
     */
    public static IndexConfig createIndexConfig(IndexType type, String... attributes) {
        IndexConfig res = new IndexConfig();

        res.setType(type);

        checkNotNull(attributes, "Index attributes cannot be null.");

        for (String attribute : attributes) {
            res.addAttribute(attribute);
        }

        return res;
    }

    /**
     * Create simple index definition with the given attributes and initialize it's name upfront. For testing purposes.
     *
     * @param type Index type.
     * @param attributes Attribute names.
     * @return Index definition.
     */
    public static IndexConfig createTestIndexConfig(IndexType type, String... attributes) {
        IndexConfig res = createIndexConfig(type, attributes);

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

            gen.open("attributes");
            for (String attribute : indexCfg.getAttributes()) {
                gen.node("attribute", attribute);
            }
            gen.close();

            if (indexCfg.getType() == IndexType.BITMAP) {
                BitmapIndexOptions bitmapIndexOptions = indexCfg.getBitmapIndexOptions();

                gen.open("bitmap-index-options");
                gen.node("unique-key", bitmapIndexOptions.getUniqueKey());
                gen.node("unique-key-transformation", bitmapIndexOptions.getUniqueKeyTransformation());
                gen.close();
            }

            gen.close();
        }
        gen.close();
    }

    public static IndexConfig getIndexConfigFromXml(Node indexNode, boolean domLevel3, boolean strict) {
        NamedNodeMap attrs = indexNode.getAttributes();

        String name = getTextContent(attrs.getNamedItem("name"), domLevel3);

        if (name.isEmpty()) {
            name = null;
        }

        String typeStr = getTextContent(attrs.getNamedItem("type"), domLevel3);
        IndexType type = getIndexTypeFromXmlName(typeStr);

        IndexConfig res = new IndexConfig().setName(name).setType(type);

        for (Node attributesNode : childElements(indexNode)) {
            if ("attributes".equals(cleanNodeName(attributesNode))) {
                for (Node attributeNode : childElements(attributesNode)) {
                    if ("attribute".equals(cleanNodeName(attributeNode))) {
                        String attribute = getTextContent(attributeNode, domLevel3);

                        res.addAttribute(attribute);
                    }
                }
            }
        }

        if (type == IndexType.BITMAP) {
            Node optionsNode = childElementWithName(indexNode, "bitmap-index-options", strict);
            if (optionsNode != null) {
                Node uniqueKeyNode = childElementWithName(optionsNode, "unique-key", strict);
                String uniqueKeyText = getTextContent(uniqueKeyNode, domLevel3);
                String uniqueKey = isNullOrEmpty(uniqueKeyText) ? BitmapIndexOptions.DEFAULT_UNIQUE_KEY : uniqueKeyText;

                Node uniqueKeyTransformationNode = childElementWithName(
                  optionsNode, "unique-key-transformation", strict);
                String uniqueKeyTransformationText = getTextContent(uniqueKeyTransformationNode, domLevel3);
                UniqueKeyTransformation uniqueKeyTransformation = isNullOrEmpty(uniqueKeyTransformationText)
                        ? BitmapIndexOptions.DEFAULT_UNIQUE_KEY_TRANSFORMATION
                        : UniqueKeyTransformation.fromName(uniqueKeyTransformationText);

                res.getBitmapIndexOptions().setUniqueKey(uniqueKey);
                res.getBitmapIndexOptions().setUniqueKeyTransformation(uniqueKeyTransformation);
            }
        }

        return res;
    }

    public static IndexType getIndexTypeFromXmlName(String typeStr) {
        if (isNullOrEmpty(typeStr)) {
            typeStr = IndexConfig.DEFAULT_TYPE.name();
        }

        if (equalsIgnoreCase(typeStr, IndexType.SORTED.name())) {
            return IndexType.SORTED;
        } else if (equalsIgnoreCase(typeStr, IndexType.HASH.name())) {
            return IndexType.HASH;
        } else if (equalsIgnoreCase(typeStr, IndexType.BITMAP.name())) {
            return IndexType.BITMAP;
        } else {
            throw new IllegalArgumentException("Unsupported index type: " + typeStr);
        }
    }

    public static IndexConfig getIndexConfigFromYaml(Node indexNode, boolean domLevel3, boolean strict) {
        NamedNodeMap attrs = indexNode.getAttributes();

        String name = getTextContent(attrs.getNamedItem("name"), domLevel3);

        if (name.isEmpty()) {
            name = null;
        }

        String typeStr = getTextContent(attrs.getNamedItem("type"), domLevel3);

        if (typeStr.isEmpty()) {
            typeStr = IndexConfig.DEFAULT_TYPE.name();
        }

        IndexType type = getIndexTypeFromXmlName(typeStr);

        IndexConfig res = new IndexConfig().setName(name).setType(type);

        Node attributesNode = attrs.getNamedItem("attributes");
        for (Node attributeNode : childElements(attributesNode)) {
            String attribute = attributeNode.getNodeValue();

            res.addAttribute(attribute);
        }

        if (type == IndexType.BITMAP) {
            Node optionsNode = childElementWithName(indexNode, "bitmap-index-options", strict);
            if (optionsNode != null) {
                Node uniqueKeyNode = childElementWithName(optionsNode, "unique-key", strict);
                String uniqueKeyText = getTextContent(uniqueKeyNode, domLevel3);
                String uniqueKey = isNullOrEmpty(uniqueKeyText) ? BitmapIndexOptions.DEFAULT_UNIQUE_KEY : uniqueKeyText;

                Node uniqueKeyTransformationNode = childElementWithName(
                  optionsNode, "unique-key-transformation", strict);
                String uniqueKeyTransformationText = getTextContent(uniqueKeyTransformationNode, domLevel3);
                UniqueKeyTransformation uniqueKeyTransformation = isNullOrEmpty(uniqueKeyTransformationText)
                        ? BitmapIndexOptions.DEFAULT_UNIQUE_KEY_TRANSFORMATION
                        : UniqueKeyTransformation.fromName(uniqueKeyTransformationText);

                res.getBitmapIndexOptions().setUniqueKey(uniqueKey);
                res.getBitmapIndexOptions().setUniqueKeyTransformation(uniqueKeyTransformation);
            }
        }

        return res;
    }

    private static String getIndexTypeName(IndexType type) {
        switch (type) {
            case SORTED:
                return "sorted";

            case HASH:
                return "hash";

            case BITMAP:
                return "bitmap";

            default:
                throw new IllegalArgumentException("Unsupported index type: " + type);
        }
    }
}
