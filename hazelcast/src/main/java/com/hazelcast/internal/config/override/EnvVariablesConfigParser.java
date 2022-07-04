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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.hazelcast.internal.util.StringUtil;

import static java.util.stream.Collectors.toMap;

/**
 * A utility class converting raw input properties into valid config entries.
 */
class EnvVariablesConfigParser {
    private static final List<String> EXCLUDED_ENTRIES = Arrays.asList(
      "HZ_HOME", "HZ_LICENSE_KEY", "HZ_PHONE_HOME_ENABLED", "HZ_CP_MOUNT");

    private final String prefix;
    private final String rootNode;

    EnvVariablesConfigParser(String prefix, String rootNode) {
        this.prefix = prefix;
        this.rootNode = rootNode;
    }

    static EnvVariablesConfigParser client() {
        return new EnvVariablesConfigParser("HZCLIENT_", "hazelcast-client");
    }

    static EnvVariablesConfigParser member() {
        return new EnvVariablesConfigParser("HZ_", "hazelcast");
    }

    Map<String, String> parse(Map<String, String> env) {
        return env.entrySet()
          .stream()
          .filter(e -> !EXCLUDED_ENTRIES.contains(e.getKey().replace(" ", "")))
          .filter(e -> e.getKey().startsWith(prefix))
          .collect(toMap(this::processKey, Map.Entry::getValue));
    }

    private String processKey(Map.Entry<String, String> e) {
        return StringUtil.lowerCaseInternal(e.getKey()
          .replaceFirst(prefix, rootNode + ".")
          .replace("_", ".")
          .replace(" ", ""));
    }
}
