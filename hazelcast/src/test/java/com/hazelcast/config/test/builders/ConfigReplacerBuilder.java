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

package com.hazelcast.config.test.builders;

import java.util.HashMap;
import java.util.Map;

public class ConfigReplacerBuilder {
        private String className;
        private Map<String, String> properties = new HashMap<>();

        private ConfigReplacerBuilder withClassName(String className) {
            this.className = className;
            return this;
        }

        public ConfigReplacerBuilder withClass(Class<?> c) {
            return withClassName(c.getName());
        }

        public ConfigReplacerBuilder addProperty(String key, boolean value) {
            return addProperty(key, String.valueOf(value));
        }

        public ConfigReplacerBuilder addProperty(String key, int value) {
            return addProperty(key, String.valueOf(value));
        }

        public ConfigReplacerBuilder addProperty(String key, String value) {
            properties.put(key, value);
            return this;
        }

        public String build() {
            if (properties.isEmpty()) {
                return "<replacer class-name='" + className + "' />\n";
            } else {
                return "<replacer class-name='" + className + "'>\n"
                        + "<properties>"
                        + properties()
                        + "</properties>"
                        + "</replacer>";
            }
        }

        private String properties() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> property : properties.entrySet()) {
                sb.append("<property name='")
                        .append(property.getKey())
                        .append("'>")
                        .append(property.getValue())
                        .append("</property>\n");
            }
            return sb.toString();
        }
    }
