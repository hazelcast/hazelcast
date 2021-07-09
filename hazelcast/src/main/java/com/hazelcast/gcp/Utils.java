/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.gcp;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Utility methods.
 */
final class Utils {
    private Utils() {
    }

    static List<String> splitByComma(String string) {
        if (string == null) {
            return Collections.emptyList();
        }
        return asList(string.trim().split("\\s*,\\s*"));
    }

    static String lastPartOf(String string, String separator) {
        String[] parts = string.split(separator);
        return parts[parts.length - 1];
    }
}
