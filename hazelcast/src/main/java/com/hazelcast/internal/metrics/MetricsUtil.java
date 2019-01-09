/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    public static String escapeMetricNamePart(@Nonnull String namePart) {
        int i = 0;
        int l = namePart.length();
        while (i < l) {
            char ch = namePart.charAt(i);
            if (ch == ',' || ch == '=' || ch == '\\') {
                break;
            }
            i++;
        }
        if (i == l) {
            return namePart;
        }
        StringBuilder sb = new StringBuilder(namePart.length() + 3);
        sb.append(namePart, 0, i);
        while (i < l) {
            char ch = namePart.charAt(i++);
            if (ch == ',' || ch == '=' || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    @SuppressFBWarnings(value = "ES_COMPARING_PARAMETER_STRING_WITH_EQ", justification = "it's intentional")
    public static boolean containsSpecialCharacters(String namePart) {
        // escapeMetricNamePart method returns input object of no escaping is needed,
        // we assume that.
        //noinspection StringEquality
        return escapeMetricNamePart(namePart) == namePart;
    }

    /**
     * Parses metric name in tagged format into a list of tag-value tuples.
     * Correctly handles the escaped values.
     *
     * @throws IllegalArgumentException if the metricName format is invalid
     * (missing square bracket, invalid escape, empty tag name...)
     */
    @Nonnull
    public static List<Map.Entry<String, String>> parseMetricName(@Nonnull String metricName) {
        if (metricName.charAt(0) != '[' || metricName.charAt(metricName.length() - 1) != ']') {
            throw new IllegalArgumentException("key not enclosed in []: " + metricName);
        }

        StringBuilder sb = new StringBuilder();
        List<Map.Entry<String, String>> result = new ArrayList<Map.Entry<String, String>>();
        int l = metricName.length() - 1;
        String tag = null;
        boolean inTag = true;
        for (int i = 1; i < l; i++) {
            char ch = metricName.charAt(i);
            switch (ch) {
                case '=':
                    tag = handleEqualsSign(metricName, sb, inTag);
                    inTag = false;
                    continue;

                case ',':
                    handleComma(metricName, sb, result, tag, inTag);
                    inTag = true;
                    tag = null;
                    continue;

                default:
                    if (ch == '\\') {
                        // the next character will be treated literally
                        ch = metricName.charAt(++i);
                    }
                    if (i == l) {
                        throw new IllegalArgumentException("backslash at the end: " + metricName);
                    }
                    sb.append(ch);
            }
        }
        // final entry
        if (tag != null) {
            result.add(new SimpleImmutableEntry<String, String>(tag, sb.toString()));
        } else if (sb.length() > 0) {
            throw new IllegalArgumentException("unfinished tag at the end: " + metricName);
        }
        return result;
    }

    private static void handleComma(@Nonnull String metricKey, StringBuilder sb, List<Entry<String, String>> result,
                                    String tag, boolean inTag) {
        if (inTag) {
            throw new IllegalArgumentException("comma in tag: " + metricKey);
        }
        result.add(new SimpleImmutableEntry<String, String>(tag, sb.toString()));
        sb.setLength(0);
    }

    private static String handleEqualsSign(@Nonnull String metricKey, StringBuilder sb, boolean inTag) {
        String tag;
        if (!inTag) {
            throw new IllegalArgumentException("equals sign not after tag: " + metricKey);
        }
        tag = sb.toString();
        if (tag.length() == 0) {
            throw new IllegalArgumentException("empty tag name: " + metricKey);
        }
        sb.setLength(0);
        return tag;
    }
}
