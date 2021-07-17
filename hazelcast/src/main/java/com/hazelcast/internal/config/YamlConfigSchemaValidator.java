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

package com.hazelcast.internal.config;

import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlToJsonConverter;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.Collections.unmodifiableList;

public class YamlConfigSchemaValidator {

    private static final List<String> PERMITTED_ROOT_NODES = unmodifiableList(
            asList("hazelcast", "hazelcast-client", "hazelcast-client-failover"));

    private static final Schema SCHEMA = SchemaLoader.builder().draftV6Support()
            .schemaJson(readJSONObject("/hazelcast-config-" + Versions.CURRENT_CLUSTER_VERSION.getMajor() + "."
                    + Versions.CURRENT_CLUSTER_VERSION.getMinor() + ".json"))
            .build()
            .load()
            .build();

    /**
     * Converts the validation exception thrown by the everit-org/json-schema library to its HZ-specific representation.
     */
    SchemaViolationConfigurationException wrap(ValidationException e) {
        List<SchemaViolationConfigurationException> subErrors = e.getCausingExceptions()
                .stream()
                .map(this::wrap)
                .collect(toList());
        return new SchemaViolationConfigurationException(e.getErrorMessage(), e.getSchemaLocation(),
                e.getPointerToViolation(), subErrors);
    }

    private static JSONObject readJSONObject(String absPath) {
        return new JSONObject(new JSONTokener(YamlConfigSchemaValidator.class.getResourceAsStream(absPath)));
    }

    /**
     * @param rootNode the root object of the configuration (should have a single "hazelcast" key)
     * @throws SchemaViolationConfigurationException if the configuration doesn't match the schema
     */
    public void validate(YamlMapping rootNode) {
        try {
            // this could be expressed in the schema as well, but that would make all the schema validation errors much harder
            // to read, so it is better to implement it here as a semantic check
            List<String> definedRootNodes = PERMITTED_ROOT_NODES.stream()
                    .filter(rootNodeName -> rootNode.child(rootNodeName) != null)
                    .collect(toList());
            if (definedRootNodes.size() != 1) {
                throw new SchemaViolationConfigurationException(
                        "exactly one of [hazelcast], [hazelcast-client] and [hazelcast-client-failover] should be present in the"
                                + " root schema document, " + definedRootNodes + " are present",
                        "#", "#", emptyList());
            } else {
                validateAdditionalProperties(rootNode, definedRootNodes.get(0));
            }
            SCHEMA.validate(YamlToJsonConverter.convert(rootNode));
        } catch (ValidationException e) {
            throw wrap(e);
        }
    }

    private void validateAdditionalProperties(YamlMapping rootNode, String hzConfigRootNodeName) {
        if (!PERMITTED_ROOT_NODES.contains(hzConfigRootNodeName)) {
            throw new IllegalArgumentException(hzConfigRootNodeName);
        }
        ObjectSchema schema = (ObjectSchema) SCHEMA;
        Set<String> forbiddenRootPropNames = ((ObjectSchema) schema.getPropertySchemas()
                        .get(hzConfigRootNodeName)).getPropertySchemas().keySet();
        List<String> misIndentedRootProps = new ArrayList<>();
        rootNode.children().forEach(yamlNode -> {
            if (forbiddenRootPropNames.contains(yamlNode.nodeName())) {
                misIndentedRootProps.add(yamlNode.nodeName());
            }
        });
        if (misIndentedRootProps.isEmpty()) {
            return;
        }
        if (misIndentedRootProps.size() == 1) {
            String propName = misIndentedRootProps.get(0);
            throw createExceptionForMisIndentedConfigProp(propName);
        } else {
            List<SchemaViolationConfigurationException> causes = misIndentedRootProps.stream()
                    .map(this::createExceptionForMisIndentedConfigProp)
                    .collect(toList());
            throw new SchemaViolationConfigurationException(causes.size() + " schema violations found", "#", "#", causes);
        }
    }

    @NotNull
    private SchemaViolationConfigurationException createExceptionForMisIndentedConfigProp(String propName) {
        return new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: ["
                + propName + "]", "#", "#", emptyList());
    }
}
