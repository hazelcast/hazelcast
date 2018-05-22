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

package com.hazelcast.internal.metrics;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A utility to escape and parse metric key in tagged format:<pre>
 *     [tag1=value1,tag2=value2,...]
 * </pre>
 */
public final class MetricsUtil {

    private MetricsUtil() {
    }

    /**
     * Escapes a user-supplied string value that is to be used as metrics key
     * tag or value.
     * <p>
     * Prefixes comma ({@code ","}), equals sign ({@code "="}) and backslash
     * ({@code "\"}) with another backslash.
     */
    @Nonnull
    public static String escapeMetricKeyPart(@Nonnull String s) {
        int i = 0;
        int l = s.length();
        while (i < l) {
            char ch = s.charAt(i);
            if (ch == ',' || ch == '=' || ch == '\\') {
                break;
            }
            i++;
        }
        if (i == l) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length() + 3);
        sb.append(s, 0, i);
        while (i < l) {
            char ch = s.charAt(i++);
            if (ch == ',' || ch == '=' || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    /**
     * Parses metric key in tagged format into a list of tag-value tuples.
     * Correctly handles the escaped values.
     *
     * @throws IllegalArgumentException if the metricKey format is invalid
     * (invalid escape, missing square bracket, empty tag name...)
     */
    @Nonnull
    public static List<Map.Entry<String, String>> parseMetricKey(@Nonnull String metricKey) {
        assert metricKey.charAt(0) == '[' && metricKey.charAt(metricKey.length() - 1) == ']' :
                "key not enclosed in []: " + metricKey;

        StringBuilder sb = new StringBuilder();
        List<Map.Entry<String, String>> result = new ArrayList<Map.Entry<String, String>>();
        int l = metricKey.length() - 1;
        String tag = null;
        boolean inTag = true;
        for (int i = 1; i < l; i++) {
            char ch = metricKey.charAt(i);
            switch (ch) {
                case '=':
                    if (!inTag) {
                        throw new IllegalArgumentException("equals sign not after tag: " + metricKey);
                    }
                    tag = sb.toString();
                    if (tag.length() == 0) {
                        throw new IllegalArgumentException("empty tag name: " + metricKey);
                    }
                    sb.setLength(0);
                    inTag = false;
                    continue;

                case ',':
                    if (inTag) {
                        throw new IllegalArgumentException("comma in tag: " + metricKey);
                    }
                    result.add(new SimpleImmutableEntry<String, String>(tag, sb.toString()));
                    sb.setLength(0);
                    inTag = true;
                    tag = null;
                    continue;

                default:
                    if (ch == '\\') {
                        // the next character will be treated literally
                        ch = metricKey.charAt(++i);
                    }
                    if (i == l) {
                        throw new IllegalArgumentException("backslash at the end: " + metricKey);
                    }
                    sb.append(ch);
            }
        }
        // final entry
        if (tag != null) {
            result.add(new SimpleImmutableEntry<String, String>(tag, sb.toString()));
        } else if (sb.length() > 0) {
            throw new IllegalArgumentException("unfinished tag at the end: " + metricKey);
        }
        return result;
    }
}
