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

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.spi.properties.HazelcastProperty;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

interface YamlConfigSchemaValidator {

    class YamlConfigSchemaValidatorImpl
            implements YamlConfigSchemaValidator {

        private static final Schema SCHEMA = SchemaLoader.builder().draftV6Support()
                .schemaJson(readJSONObject("/hazelcast-config-4.2.json"))
                .build()
                .load().build();

        private static JSONObject readJSONObject(String absPath) {
            return new JSONObject(new JSONTokener(YamlConfigSchemaValidator.class.getResourceAsStream(absPath)));
        }

        @Override
        public void validate(YamlMapping rootNode) {
            SCHEMA.validate(null);
        }
    }

    /**
     * Set it to {@code true} to skip the YAML configuration validation.
     */
    HazelcastProperty SKIP_PROP = new HazelcastProperty("yaml.config.validation.skip", "false");

    static YamlConfigSchemaValidator create() {
        if ("true".equalsIgnoreCase(SKIP_PROP.getSystemProperty())) {
            return rootNode -> {
            };
        }
        return new YamlConfigSchemaValidatorImpl();
    }

    void validate(YamlMapping rootNode);
}
