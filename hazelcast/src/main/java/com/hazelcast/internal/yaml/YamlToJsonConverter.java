/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Used to convert YAML configurations from their {@link YamlMapping} representation to their equivalent {@link JSONObject}
 * representation. It is necessary to convert the configuration written in YAML syntax against a JSON schema.
 */
public final class YamlToJsonConverter {

    private YamlToJsonConverter() {
        // util class
    }

    public static Object convert(YamlNode yamlNode) {
        if (yamlNode == null) {
            return JSONObject.NULL;
        }
        if (yamlNode instanceof YamlMapping) {
            YamlMapping yamlMapping = (YamlMapping) yamlNode;
            JSONObject resultObject = new JSONObject();
            for (YamlNameNodePair pair : yamlMapping.childrenPairs()) {
                resultObject.put(pair.nodeName(), convert(pair.childNode()));
            }
            return resultObject;
        } else if (yamlNode instanceof YamlSequence) {
            YamlSequence yamlSequence = (YamlSequence) yamlNode;
            JSONArray resultArray = new JSONArray();
            for (YamlNode child : yamlSequence.children()) {
                resultArray.put(convert(child));
            }
            return resultArray;
        } else if (yamlNode instanceof YamlScalar) {
            return convertScalar((YamlScalar) yamlNode);
        }
        throw new IllegalArgumentException("Unknown type " + yamlNode.getClass().getName());
    }

    /**
     * This method makes the json schema validation lenient in a sense that it converts string values to booleans and integers,
     * if they are parseable as such. This is necessary to support configreplacers in the YAML config: when a placeholder is
     * replaced with an actual value, then the replacement is always a string, even if it holds an integer or boolean value, so we
     * convert it here on a best-effort basis, otherwise the schema validation would fail errors like "expected type: integer,
     * actual: string" even in cases when eg. ${backup-count} is substituted with "1".
     */
    private static Object convertScalar(YamlScalar yamlNode) {
        Object rawNodeValue = yamlNode.nodeValue();
        if (rawNodeValue instanceof String) {
            String stringValue = (String) rawNodeValue;
            if (stringValue.equals("true")) {
                return Boolean.TRUE;
            } else if (stringValue.equals("false")) {
                return Boolean.FALSE;
            }
            try {
                return new Integer(stringValue);
            } catch (NumberFormatException e) {
                try {
                    return new Float(stringValue);
                } catch (NumberFormatException e2) {
                    // returning rawNodeValue
                }
            }
        }
        return rawNodeValue;
    }

}
