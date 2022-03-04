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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validation mechanism used to make sure
 * that external configuration (coming from env/system properties) features is valid.
 */
final class ConfigOverrideValidator {

    private static final ILogger LOGGER = Logger.getLogger(ConfigOverrideValidator.class);

    private ConfigOverrideValidator() {
    }

    static void validate(Set<ConfigProvider> providers) {
        Set<String> sharedKeys = findDuplicateEntries(providers);
        if (!sharedKeys.isEmpty()) {
            LOGGER.severe("Discovered conflicting entries: " + String.join(",", sharedKeys));
            throw new InvalidConfigurationException("Discovered conflicting configuration entries");
        }
    }

    private static Set<String> findDuplicateEntries(Set<ConfigProvider> providers) {
        return providers.stream()
          .flatMap(p -> p.properties().keySet().stream())
          .map(e -> e.replace("-", ""))
          .collect(Collectors.groupingBy(i -> i, Collectors.counting()))
          .entrySet().stream()
          .filter(e -> e.getValue() > 1)
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());
    }
}
