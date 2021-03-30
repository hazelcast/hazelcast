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

public final class YamlConverter {

    private YamlConverter() {
        // util class
    }

    public static Object convertToJson(YamlNode yamlNode) {
        if (yamlNode instanceof YamlMapping) {
            YamlMapping yamlMapping = (YamlMapping) yamlNode;
            JSONObject resultObject = new JSONObject();
            for (YamlNode child : yamlMapping.children()) {
                resultObject.put(child.nodeName(), convertToJson(child));
            }
            return resultObject;
        }  else if (yamlNode instanceof YamlSequence) {
            YamlSequence yamlSequence = (YamlSequence) yamlNode;
            JSONArray resultArray = new JSONArray();
            for (YamlNode child : yamlSequence.children()) {
                resultArray.put(convertToJson(child));
            }
            return resultArray;
        }   else if (yamlNode instanceof YamlScalar) {
            YamlScalar yamlScalar = (YamlScalar) yamlNode;
            return yamlScalar.nodeValue();
        }
        throw new IllegalArgumentException("Unknown type");
    }

}
