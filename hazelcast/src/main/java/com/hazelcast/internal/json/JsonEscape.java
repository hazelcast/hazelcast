/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.json;

import static com.hazelcast.internal.json.JsonWriter.getReplacementChars;

/**
 * Utility method to escape characters for fields used in a Json.
 */
public class JsonEscape {

    public static void writeEscaped(StringBuilder stringBuilder, String string) {
        stringBuilder.append('"');
        int length = string.length();
        int start = 0;
        for (int index = 0; index < length; index++) {
            char[] replacement = getReplacementChars(string.charAt(index));
            if (replacement != null) {
                stringBuilder.append(string, start, index);
                stringBuilder.append(replacement);
                start = index + 1;
            }
        }
        stringBuilder.append(string, start, length);
        stringBuilder.append('"');
    }

    public static void writeEscaped(StringBuilder stringBuilder, char c) {
        stringBuilder.append('"');
        char[] replacement = getReplacementChars(c);
        if (replacement != null) {
            stringBuilder.append(replacement);
        } else {
            stringBuilder.append(c);
        }
        stringBuilder.append('"');
    }
}
