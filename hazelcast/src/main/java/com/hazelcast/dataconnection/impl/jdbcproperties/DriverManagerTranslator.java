/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.impl.jdbcproperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DriverManagerTranslator {

    private static final Map<String, String> PROPERTY_MAP = new HashMap<>();

    static {
        PROPERTY_MAP.put(DataConnectionProperties.USER, "user");
        PROPERTY_MAP.put(DataConnectionProperties.PASSWORD, "password");
    }

    public Properties translate(Properties source) {
        Properties driverManagerProperties = new Properties();

        // Iterate over source Properties and translate to Hikari
        source.forEach((key, value) -> {

            String translatedProperty = PROPERTY_MAP.get(key);
            if (translatedProperty != null) {
                // We can translate from HZ to DriverManager
                driverManagerProperties.put(translatedProperty, value);
            } else {
                // Otherwise pass as is
                driverManagerProperties.put(key, value);
            }
        });

        driverManagerProperties.remove("jdbcUrl");

        return driverManagerProperties;
    }
}
