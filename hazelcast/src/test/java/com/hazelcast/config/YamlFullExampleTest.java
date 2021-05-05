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

package com.hazelcast.config;

import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlToJsonConverter;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class YamlFullExampleTest {

    /**
     * The full-example yaml files don't validate successfully out of the box, because in their import declarations they refer to
     * non-existent yaml files, therefore {@link YamlConfigBuilder} and
     * {@link com.hazelcast.client.config.YamlClientConfigBuilder} throw exceptions while processing the imports.
     * <p>
     * To work this around, here we read the original yaml config from a stream, parse it, remove the {@code import} object,
     * then print again and return it.
     * <p>
     * Since {@link YamlMapping} can't print itself back to a YAML string, we convert the {@code YamlMapping} instance to a
     * {@code JSONObject} and use it to perform the removal of {@code import} node and serialization back to string. The result
     * will be a JSON string, but YAML is compatible with JSON, so the subsequent parsing in the ConfigBuilder under test will
     * parse it successfully.
     */
    private static InputStream removeImports(InputStream original, String rootObjName) {
        YamlMapping m = (YamlMapping) YamlLoader.load(original);
        JSONObject yamlAsJson = (JSONObject) YamlToJsonConverter.convert(m);
        yamlAsJson.getJSONObject(rootObjName).remove("import");
        return new ByteArrayInputStream(yamlAsJson.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void memberFullExampleYamlLoadsSuccessfully() {
        new YamlConfigBuilder(removeImports(getClass().getResourceAsStream("/hazelcast-full-example.yaml"), "hazelcast")).build();
    }

    @Test
    public void clientFullExampleYamlLoadsSuccessfully() {
        new YamlClientConfigBuilder(
                removeImports(getClass().getResourceAsStream("/hazelcast-client-full-example.yaml"), "hazelcast-client")
        ).build();

    }
}
