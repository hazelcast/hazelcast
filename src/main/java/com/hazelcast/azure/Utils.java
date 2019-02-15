/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.azure;

/**
 * Utility methods.
 */
final class Utils {

    private Utils() {
    }

    static boolean isBlank(final CharSequence cs) {
        int strLen = cs == null ? 0 : cs.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static boolean isAllBlank(final String... values) {
        if (values != null) {
            for (final String val : values) {
                if (!isBlank(val)) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean isAllNotBlank(final String... values) {
        if (values == null) return false;
        for (final String val : values) {
            if (isBlank(val)) {
                return false;
            }
        }
        return true;
    }
}
