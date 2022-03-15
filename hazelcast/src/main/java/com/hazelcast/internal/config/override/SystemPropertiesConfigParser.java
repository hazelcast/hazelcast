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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;

import com.hazelcast.internal.util.StringUtil;

import static java.util.stream.Collectors.toMap;

/**
 * A utility class converting raw input properties into valid config entries.
 */
class SystemPropertiesConfigParser {
    private final String prefix;
    private final String rootNode;

    SystemPropertiesConfigParser(String prefix, String rootNode) {
        this.prefix = prefix;
        this.rootNode = rootNode;
    }

    static SystemPropertiesConfigParser client() {
        return new SystemPropertiesConfigParser("hz-client.", "hazelcast-client");
    }

    static SystemPropertiesConfigParser member() {
        return new SystemPropertiesConfigParser("hz.", "hazelcast");
    }

    Map<String, String> parse(Properties properties) {
        return properties.entrySet()
          .stream()
          .map(e -> new AbstractMap.SimpleEntry<>((String) e.getKey(), (String) e.getValue()))
          .filter(e -> e.getKey().startsWith(prefix))
          .collect(toMap(this::processKey, Map.Entry::getValue));
    }

    private String processKey(AbstractMap.SimpleEntry<String, String> e) {
        return StringUtil.lowerCaseInternal(e.getKey()
          .replace(" ", "")
          .replaceFirst(prefix, rootNode + "."));
    }
}
