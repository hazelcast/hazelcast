/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class VectorStringUtil {
    private VectorStringUtil() {
    }

    private static String toString(Object value) {
        if (value instanceof float[] array) {
            return Arrays.toString(array);
        } else {
            return Objects.toString(value);
        }
    }

    public static String mapToString(Map<String, ?> data) {
        if (data == null) {
            return "null";
        }
        String dataString = data.keySet().stream()
                .map(key -> key + "=" + toString(data.get(key)))
                .collect(Collectors.joining(", ", "{", "}"));
        return dataString;
    }
}
