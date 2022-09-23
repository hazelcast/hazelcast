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

package com.hazelcast.internal.config;

import com.hazelcast.client.config.impl.ClientConfigSections;
import com.hazelcast.client.config.impl.ClientFailoverConfigSections;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlToJsonConverter;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.PrimitiveValidationStrategy;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.Validator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class YamlConfigSchemaValidator {

    private static final HazelcastProperty ROOT_LEVEL_INDENTATION_CHECK_ENABLED
            = new HazelcastProperty("hazelcast.yaml.config.indentation.check.enabled", "true");

    private static final List<String> PERMITTED_ROOT_NODES = unmodifiableList(
            asList(ConfigSections.HAZELCAST.getName(), ClientConfigSections.HAZELCAST_CLIENT.getName(),
                    ClientFailoverConfigSections.CLIENT_FAILOVER.getName()));

    private static final Schema SCHEMA;

    static {
        String absPath = "/hazelcast-config-" + Versions.CURRENT_CLUSTER_VERSION.getMajor() + "."
                + Versions.CURRENT_CLUSTER_VERSION.getMinor() + ".json";

        SCHEMA = SchemaLoader.builder().draftV6Support()
                .schemaJson(readJSONObject(absPath))
                .build()
                .load()
                .build();
    }

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
                    .filter(rootNodeName -> rootNode != null
                            && StreamSupport.stream(rootNode.childrenPairs().spliterator(), false)
                                            .anyMatch(pair -> pair.nodeName().equals(rootNodeName)))
                    .collect(toList());
            if (definedRootNodes.size() != 1) {
                throw new SchemaViolationConfigurationException(
                        "exactly one of [hazelcast], [hazelcast-client] and [hazelcast-client-failover] should be present in the"
                                + " root schema document, " + definedRootNodes.size() + " are present",
                        "#", "#", emptyList());
            } else if (new HazelcastProperties(System.getProperties()).getBoolean(ROOT_LEVEL_INDENTATION_CHECK_ENABLED)) {
                validateAdditionalProperties(rootNode, definedRootNodes.get(0));
            }
            // Make the root node nullable by skipping validation when the value
            // of root node is null. When changing the root element in the json schema
            // to nullable, it significantly reduces the readability of the validation
            // error messages, so we preferred this workaround.
            if (rootNode != null
                    && rootNode.childCount() == 1
                    && rootNode.child(definedRootNodes.get(0)) == null
            ) {
                return;
            }

            Validator.builder()
                    .primitiveValidationStrategy(PrimitiveValidationStrategy.LENIENT)
                    .build()
                    .performValidation(SCHEMA, YamlToJsonConverter.convert(rootNode));
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
            throw createExceptionForMisIndentedConfigProp(propName, true);
        } else {
            List<SchemaViolationConfigurationException> causes = misIndentedRootProps.stream()
                    .map(prop -> createExceptionForMisIndentedConfigProp(prop, false))
                    .collect(toList());
            throw new SchemaViolationConfigurationException(withNote(causes.size() + " schema violations found"),
                    "#", "#", causes);
        }
    }

    private static String withNote(String originalMessage) {
        return originalMessage + System.getProperty("line.separator") + "Note: you can disable this validation by passing the "
                + "-D" + ROOT_LEVEL_INDENTATION_CHECK_ENABLED.getName() + "=false system property";
    }

    private SchemaViolationConfigurationException createExceptionForMisIndentedConfigProp(String propName, boolean addNote) {
        String message = "Mis-indented hazelcast configuration property found: [" + propName + "]";
        if (addNote) {
            message = withNote(message);
        }
        return new SchemaViolationConfigurationException(message, "#", "#", emptyList());
    }
}
