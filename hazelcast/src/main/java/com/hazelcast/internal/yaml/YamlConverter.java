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
