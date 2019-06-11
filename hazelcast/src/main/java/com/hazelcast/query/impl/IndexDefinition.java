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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static com.hazelcast.query.impl.predicates.PredicateUtils.canonicalizeAttribute;
import static com.hazelcast.query.impl.predicates.PredicateUtils.constructCanonicalCompositeIndexName;

public final class IndexDefinition {

    private static final int MAX_INDEX_COMPONENTS = 255;

    private static final Pattern COMMA_PATTERN = Pattern.compile("\\s*,\\s*");

    private static final Pattern ARROW_PATTERN = Pattern.compile("\\s*->\\s*");

    public static IndexDefinition parse(String definition, boolean ordered) {
        IndexDefinition parsedDefinition = tryParseCompact(definition, ordered);
        if (parsedDefinition != null) {
            return parsedDefinition;
        }

        parsedDefinition = tryParseComposite(definition, ordered);
        if (parsedDefinition != null) {
            return parsedDefinition;
        }

        String attribute = canonicalizeAttribute(definition);
        return new IndexDefinition(attribute, ordered, null, attribute);
    }

    private static IndexDefinition tryParseCompact(String definition, boolean ordered) {
        String[] parts = ARROW_PATTERN.split(definition);

        if (parts.length == 1) {
            return null;
        }

        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid compact index definition: " + definition);
        }

        parts[0] = canonicalizeAttribute(parts[0]);
        parts[1] = canonicalizeAttribute(parts[1]);

        if (parts[0].isEmpty() || parts[1].isEmpty() || parts[0].equals(parts[1])) {
            throw new IllegalArgumentException("Invalid compact index definition: " + definition);
        }

        if (parts[0].contains(",") || parts[1].contains(",")) {
            throw new IllegalArgumentException("Composite compact indexes are not supported: " + definition);
        }

        return new IndexDefinition(parts[0] + " -> " + parts[1], ordered, parts[1], parts[0]);
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

        return new IndexDefinition(constructCanonicalCompositeIndexName(attributes), ordered, null, attributes);
    }

    private final String name;
    private final boolean ordered;
    private final String uniqueKey;
    private final String[] components;

    private IndexDefinition(String name, boolean ordered, String uniqueKey, String... components) {
        this.name = name;
        this.ordered = ordered;
        this.uniqueKey = uniqueKey;
        this.components = components;
    }

    public String getName() {
        return name;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public String[] getComponents() {
        return components;
    }

}
