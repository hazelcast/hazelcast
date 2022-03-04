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
package com.hazelcast.internal.config.override;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link ConfigProvider} extracting config entries from system properties.
 */
class SystemPropertiesConfigProvider implements ConfigProvider {

    private final SystemPropertiesConfigParser systemPropertiesConfigParser;
    private final SystemPropertiesProvider systemPropertiesProvider;

    SystemPropertiesConfigProvider(SystemPropertiesConfigParser systemPropertiesConfigParser,
                                   SystemPropertiesProvider systemPropertiesProvider) {
        this.systemPropertiesConfigParser = systemPropertiesConfigParser;
        this.systemPropertiesProvider = systemPropertiesProvider;
    }

    @Override
    public Map<String, String> properties() {
        return Collections.unmodifiableMap(systemPropertiesConfigParser.parse(systemPropertiesProvider.get()));
    }

    @Override
    public String name() {
        return "system properties";
    }

    @FunctionalInterface
    interface SystemPropertiesProvider {
        Properties get();
    }
}
