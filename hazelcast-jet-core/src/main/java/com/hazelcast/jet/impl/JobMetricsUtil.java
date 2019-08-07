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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.metrics.MetricTags;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class JobMetricsUtil {

    private static final Pattern METRIC_KEY_EXEC_ID_PATTERN =
            Pattern.compile("\\[module=jet,job=[^,]+,exec=([^,]+),.*");

    // required precision after the decimal point for doubles
    private static final int CONVERSION_PRECISION = 4;
    // coefficient for converting doubles to long
    private static final double DOUBLE_TO_LONG = Math.pow(10, CONVERSION_PRECISION);

    private JobMetricsUtil() {
    }

    public static Long getExecutionIdFromMetricDescriptor(@Nonnull String descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");

        Matcher m = METRIC_KEY_EXEC_ID_PATTERN.matcher(descriptor);
        if (!m.matches()) {
            // not a job-related metric, ignore it
            return null;
        }
        return Util.idFromString(m.group(1));
    }

    public static long toLongMetricValue(double value) {
        return Math.round(value * DOUBLE_TO_LONG);
    }

    public static Map<String, String> parseMetricDescriptor(@Nonnull String descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");

        return MetricsUtil.parseMetricName(descriptor).stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static String addPrefixToDescriptor(@Nonnull String descriptor, @Nonnull String prefix) {
        assert !prefix.isEmpty() : "Prefix is empty";
        assert prefix.endsWith(",") : "Prefix should end in a comma";

        assert descriptor.length() >= 3 : "Descriptor too short";
        assert descriptor.startsWith("[") : "Descriptor of non-standard format";
        assert descriptor.endsWith("]") : "Descriptor of non-standard format";

        return "[" + prefix + descriptor.substring(1);
    }

    static String getMemberPrefix(@Nonnull MemberInfo memberInfo) {
        Objects.requireNonNull(memberInfo, "memberInfo");

        String uuid = memberInfo.getUuid();
        String address = memberInfo.getAddress().toString();
        return MetricTags.MEMBER + "=" + MetricsUtil.escapeMetricNamePart(uuid) + "," +
                MetricTags.ADDRESS + "=" + MetricsUtil.escapeMetricNamePart(address) + ",";
    }
}
