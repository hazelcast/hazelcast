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

import java.util.List;
import java.util.Map;

/**
 * Builds a DOM of {@link YamlNode} instances from the structure parsed
 * by SnakeYaml.
 *
 * @see YamlMapping
 * @see YamlSequence
 * @see YamlScalar
 */
public final class YamlDomBuilder {
    private YamlDomBuilder() {
    }

    static YamlNode build(Object document, String rootName) {
        if (document == null) {
            throw new YamlException("The provided document is null");
        }

        if (rootName != null && !(document instanceof Map)) {
            throw new YamlException("The provided document is not a Map, and rootName is defined.");
        }

        Object rootNode;
        if (rootName != null) {
            if (!((Map) document).containsKey(rootName)) {
                throw new YamlException("The required " + rootName
                        + " root node couldn't be found in the document root");
            }
            rootNode = ((Map) document).get(rootName);
        } else {
            rootNode = document;
        }

        return buildNode(null, rootName, rootNode);
    }

    public static YamlNode build(Object document) {
        return build(document, null);
    }

    @SuppressWarnings("unchecked")
    private static YamlNode buildNode(YamlNode parent, String nodeName, Object sourceNode) {
        if (sourceNode == null) {
            return null;
        }

        if (sourceNode instanceof Map) {
            YamlMappingImpl node = new YamlMappingImpl(parent, nodeName);
            buildChildren(node, (Map<String, Object>) sourceNode);
            return node;
        } else if (sourceNode instanceof List) {
            YamlSequenceImpl node = new YamlSequenceImpl(parent, nodeName);
            buildChildren(node, (List<Object>) sourceNode);
            return node;
        } else if (isSupportedScalarType(sourceNode)) {
            return buildScalar(parent, nodeName, sourceNode);
        } else {
            throw new YamlException("An unsupported scalar type is encountered: " + nodeName + " is an instance of "
                    + sourceNode.getClass().getName()
                    + ". The supported types are String, Integer, Long, Double and Boolean.");
        }
    }

    private static boolean isSupportedScalarType(Object sourceNode) {
        return sourceNode instanceof String
                || sourceNode instanceof Integer
                || sourceNode instanceof Long
                || sourceNode instanceof Double
                || sourceNode instanceof Boolean;
    }

    private static void buildChildren(YamlMappingImpl parentNode, Map<String, Object> mapNode) {
        for (Map.Entry<String, Object> entry : mapNode.entrySet()) {
            String childNodeName = entry.getKey();
            Object childNodeValue = entry.getValue();
            YamlNode child = buildNode(parentNode, childNodeName, childNodeValue);
            parentNode.addChild(childNodeName, child);
        }
    }

    private static void buildChildren(YamlSequenceImpl parentNode, List<Object> listNode) {
        for (Object value : listNode) {
            YamlNode child = buildNode(parentNode, null, value);
            parentNode.addChild(child);
        }
    }

    private static YamlNode buildScalar(YamlNode parent, String nodeName, Object value) {
        return new YamlScalarImpl(parent, nodeName, value);
    }
}
