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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlToJsonConverter;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.List;

import static java.util.stream.Collectors.toList;

interface YamlConfigSchemaValidator {

    class YamlConfigSchemaValidatorImpl
            implements YamlConfigSchemaValidator {

        private static final Schema SCHEMA = SchemaLoader.builder().draftV6Support()
                .schemaJson(readJSONObject("/hazelcast-config-" + Versions.CURRENT_CLUSTER_VERSION.getMajor() + "."
                        + Versions.CURRENT_CLUSTER_VERSION.getMinor() + ".json"))
                .build()
                .load()
                .build();

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

        @Override
        public void validate(YamlMapping rootNode) {
            try {
                SCHEMA.validate(YamlToJsonConverter.convert(rootNode));
            } catch (ValidationException e) {
                throw wrap(e);
            }
        }
    }

    static YamlConfigSchemaValidator create() {
        return new YamlConfigSchemaValidatorImpl();
    }

    /**
     * @param rootNode the root object of the configuration (should have a single "hazelcast" key)
     * @throws SchemaViolationConfigurationException if the configuration doesn't match the schema
     */
    void validate(YamlMapping rootNode);
}
