/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

class ExternalConfigTestUtils {

    static void runWithSystemProperty(String key, String value, Runnable action) {
        try {
            System.setProperty(key, value);
            action.run();
        } finally {
            System.clearProperty(key);
        }
    }

    @SafeVarargs
    static void runWithSystemProperties(Runnable action, Map.Entry<String, String>... entry) {
        try {
            for (Map.Entry<String, String> e : entry) {
                System.setProperty(e.getKey(), e.getValue());
            }
            action.run();
        } finally {
            for (Map.Entry<String, String> e : entry) {
                System.clearProperty(e.getKey());
            }
        }
    }

    static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
