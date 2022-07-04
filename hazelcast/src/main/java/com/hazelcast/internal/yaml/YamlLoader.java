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

package com.hazelcast.internal.yaml;

import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.InputStream;
import java.io.Reader;

/**
 * YAML loader that can load, parse YAML documents and can build a tree
 * of {@link YamlNode} instances.
 * <p>
 * The possible sources of the YAML documents are:
 * <ul>
 * <li>{@link InputStream}</li>
 * <li>{@link Reader}</li>
 * <li>{@link String}</li>
 * </ul>
 */
public final class YamlLoader {
    private YamlLoader() {
    }

    /**
     * Loads a YAML document from an {@link InputStream} and builds a
     * {@link YamlNode} tree that is under the provided top-level
     * {@code rootName} key. This loading mode requires the topmost level
     * of the YAML document to be a mapping.
     *
     * @param inputStream The input stream to load the YAML from
     * @param rootName    The name of the root's key
     * @return the tree built from the YAML document
     */
    public static YamlNode load(InputStream inputStream, String rootName) {
        try {
            Object document = getLoad().loadFromInputStream(inputStream);
            return buildDom(rootName, document);
        } catch (Exception ex) {
            throw new YamlException("An error occurred while loading and parsing the YAML stream", ex);
        }
    }

    /**
     * Loads a YAML document from an {@link InputStream} and builds a
     * {@link YamlNode} tree.
     *
     * @param inputStream The input stream to load the YAML from
     * @return the tree built from the YAML document
     */
    public static YamlNode load(InputStream inputStream) {
        try {
            Object document = getLoad().loadFromInputStream(inputStream);
            return buildDom(document);
        } catch (Exception ex) {
            throw new YamlException("An error occurred while loading and parsing the YAML stream", ex);
        }
    }

    /**
     * Loads a YAML document from an {@link Reader} and builds a
     * {@link YamlNode} tree that is under the provided top-level
     * {@code rootName} key. This loading mode requires the topmost level
     * of the YAML document to be a mapping.
     *
     * @param reader   The reader to load the YAML from
     * @param rootName The name of the root's key
     * @return the tree built from the YAML document
     */
    public static YamlNode load(Reader reader, String rootName) {
        try {
            Object document = getLoad().loadFromReader(reader);
            return buildDom(rootName, document);
        } catch (Exception ex) {
            throw new YamlException("An error occurred while loading and parsing the YAML stream", ex);
        }
    }

    /**
     * Loads a YAML document from an {@link Reader} and builds a
     * {@link YamlNode} tree.
     *
     * @param reader The reader to load the YAML from
     * @return the tree built from the YAML document
     */
    public static YamlNode load(Reader reader) {
        try {
            Object document = getLoad().loadFromReader(reader);
            return buildDom(document);
        } catch (Exception ex) {
            throw new YamlException("An error occurred while loading and parsing the YAML stream", ex);
        }
    }

    /**
     * Loads a YAML document from an {@link String} and builds a
     * {@link YamlNode} tree that is under the provided top-level
     * {@code rootName} key. This loading mode requires the topmost level
     * of the YAML document to be a mapping.
     *
     * @param yaml     The string to load the YAML from
     * @param rootName The name of the root's key
     * @return the tree built from the YAML document
     */
    public static YamlNode load(String yaml, String rootName) {
        try {
            Object document = getLoad().loadFromString(yaml);
            return buildDom(rootName, document);
        } catch (Exception ex) {
            throw new YamlException("An error occurred while loading and parsing the YAML string", ex);
        }
    }

    /**
     * Loads a YAML document from an {@link String} and builds a
     * {@link YamlNode} tree.
     *
     * @param yaml The string to load the YAML from
     * @return the tree built from the YAML string
     */
    public static YamlNode load(String yaml) {
        try {
            Object document = getLoad().loadFromString(yaml);
            return buildDom(document);
        } catch (Exception ex) {
            throw new YamlException("An error occurred while loading and parsing the YAML string", ex);
        }
    }

    private static Load getLoad() {
        LoadSettings settings = LoadSettings.builder().build();
        return new Load(settings);
    }

    private static YamlNode buildDom(String rootName, Object document) {
        return YamlDomBuilder.build(document, rootName);
    }

    private static YamlNode buildDom(Object document) {
        return YamlDomBuilder.build(document);
    }
}
