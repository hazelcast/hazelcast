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

import com.hazelcast.query.impl.predicates.PredicateUtils;
import com.hazelcast.util.StringUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.impl.predicates.PredicateUtils.canonicalizeAttribute;
import static com.hazelcast.query.impl.predicates.PredicateUtils.constructCanonicalCompositeIndexName;

/**
 * Defines an index.
 */
public final class IndexDefinition {

    private static final int MAX_INDEX_COMPONENTS = 255;

    private static final Pattern COMMA_PATTERN = Pattern.compile("\\s*,\\s*");

    private static final String BITMAP_PREFIX = "BITMAP(";
    private static final String BITMAP_POSTFIX = ")";

    private final String name;
    private final boolean ordered;
    private final String uniqueKey;
    private final UniqueKeyTransform uniqueKeyTransform;
    private final String[] components;

    /**
     * Defines an assortment of transforms applied to unique key values.
     */
    public enum UniqueKeyTransform {

        /**
         * Extracted unique key value is interpreted as an object value.
         * Non-negative unique ID is assigned to every distinct object value.
         */
        OBJECT("OBJECT"),

        /**
         * Extracted unique key value is interpreted as a whole integer value of
         * byte, short, int or long type. The extracted value is upcasted to
         * long and unique non-negative ID is assigned to every distinct value.
         */
        LONG("LONG"),

        /**
         * Extracted unique key value is interpreted as a whole integer value of
         * byte, short, int or long type. The extracted value is upcasted to
         * long and the resulting value is used directly as an ID.
         */
        RAW("RAW");

        private final String text;

        UniqueKeyTransform(String text) {
            this.text = text;
        }

        private static UniqueKeyTransform parse(String text) {
            if (StringUtil.isNullOrEmpty(text)) {
                throw new IllegalArgumentException("empty unique key transform");
            }

            String upperCasedText = text.toUpperCase();
            if (upperCasedText.equals(OBJECT.text)) {
                return OBJECT;
            }
            if (upperCasedText.equals(LONG.text)) {
                return LONG;
            }
            if (upperCasedText.equals(RAW.text)) {
                return RAW;
            }

            throw new IllegalArgumentException("unexpected unique key transform: " + text);
        }

        @Override
        public String toString() {
            return text;
        }
    }

    private IndexDefinition(String name, boolean ordered, String... components) {
        this.name = name;
        this.ordered = ordered;
        this.uniqueKey = null;
        this.uniqueKeyTransform = UniqueKeyTransform.OBJECT;
        this.components = components;
    }

    private IndexDefinition(String name, boolean ordered, String uniqueKey, UniqueKeyTransform uniqueKeyTransform,
                            String... components) {
        this.name = name;
        this.ordered = ordered;
        this.uniqueKey = uniqueKey;
        this.uniqueKeyTransform = uniqueKeyTransform;
        this.components = components;
    }

    /**
     * Parses the given index definition. The following definitions are
     * recognized:
     * <ul>
     * <li>Regular index definition: a single attribute path ({@code attr}).
     * <li>Composite index definition: multiple attribute paths separated by
     * commas ({@code attr1, attr2, attr3}).
     * <li>Bitmap index definition: a single attribute path followed by a unique
     * key path separated by "->" ({@code attr -> uniqueKeyAttr}).
     * </ul>
     *
     * @param definition the definition to parse.
     * @param ordered    {@code true} if the given definition should define an
     *                   ordered index, {@code false} for unordered.
     * @return the parsed out index definition.
     * @throws IllegalArgumentException if the given definition is considered
     *                                  invalid.
     */
    public static IndexDefinition parse(String definition, boolean ordered) {
        IndexDefinition parsedDefinition = tryParseBitmap(definition, ordered);
        if (parsedDefinition != null) {
            return parsedDefinition;
        }

        parsedDefinition = tryParseComposite(definition, ordered);
        if (parsedDefinition != null) {
            return parsedDefinition;
        }

        String attribute = canonicalizeAttribute(definition);
        return new IndexDefinition(attribute, ordered, attribute);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private static IndexDefinition tryParseBitmap(String definition, boolean ordered) {
        if (definition == null || !definition.toUpperCase().startsWith(BITMAP_PREFIX)) {
            return null;
        }

        if (!definition.endsWith(BITMAP_POSTFIX)) {
            throw makeInvalidBitmapIndexDefinitionException(definition);
        }

        String innerText = definition.substring(BITMAP_PREFIX.length(), definition.length() - 1);
        String[] parts = COMMA_PATTERN.split(innerText, -1);

        if (parts.length == 0 || parts.length > 3) {
            throw makeInvalidBitmapIndexDefinitionException(definition);
        }

        String attribute = canonicalizeAttribute(parts[0]);
        String uniqueKey = parts.length >= 2 ? canonicalizeAttribute(parts[1]) : KEY_ATTRIBUTE_NAME.value();
        UniqueKeyTransform uniqueKeyTransform =
                parts.length == 3 ? UniqueKeyTransform.parse(parts[2]) : UniqueKeyTransform.OBJECT;

        if (attribute.isEmpty() || uniqueKey.isEmpty() || attribute.equals(uniqueKey)) {
            throw makeInvalidBitmapIndexDefinitionException(definition);
        }

        String canonicalName = BITMAP_PREFIX + attribute + ", " + uniqueKey + ", " + uniqueKeyTransform + BITMAP_POSTFIX;
        return new IndexDefinition(canonicalName, ordered, uniqueKey, uniqueKeyTransform, attribute);
    }

    private static IllegalArgumentException makeInvalidBitmapIndexDefinitionException(String definition) {
        return new IllegalArgumentException("Invalid bitmap index definition: " + definition);
    }

    private static IndexDefinition tryParseComposite(String definition, boolean ordered) {
        String[] attributes = COMMA_PATTERN.split(definition, -1);

        if (attributes.length == 1) {
            return null;
        }

        if (attributes.length > MAX_INDEX_COMPONENTS) {
            throw new IllegalArgumentException("Too many composite index attributes: " + definition);
        }

        Set<String> seenAttributes = new HashSet<String>(attributes.length);
        for (int i = 0; i < attributes.length; ++i) {
            String component = PredicateUtils.canonicalizeAttribute(attributes[i]);
            attributes[i] = component;

            if (component.isEmpty()) {
                throw new IllegalArgumentException("Empty composite index attribute: " + definition);
            }
            if (!seenAttributes.add(component)) {
                throw new IllegalArgumentException("Duplicate composite index attribute: " + definition);
            }
        }

        return new IndexDefinition(constructCanonicalCompositeIndexName(attributes), ordered, attributes);
    }

    /**
     * @return the canonical name of this index.
     */
    public String getName() {
        return name;
    }

    /**
     * @return {@code true} if this index ordered, {@code false} if unordered.
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * @return the unique key attribute path, which may be used to extract
     * a value that uniquely identifies an entry being indexed.
     */
    public String getUniqueKey() {
        return uniqueKey;
    }

    /**
     * @return the transform that should be applied to {@link #getUniqueKey()
     * unique key} values.
     */
    public UniqueKeyTransform getUniqueKeyTransform() {
        return uniqueKeyTransform;
    }

    /**
     * @return the components of this index, which are attributes being indexed
     * by it. Regular and bitmap indexes have just a single component, while
     * composite indexes have multiple components.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public String[] getComponents() {
        return components;
    }

}
