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

package com.hazelcast.internal.util.phonehome;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

@NotThreadSafe
public class MetricsCollectionContext {
    private final Map<String, String> parameters = new LinkedHashMap<>();
    private String query;

    Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Collects the given {@code metric}. The value will be converted to string.
     */
    public void collect(@Nonnull Metric metric, @Nonnull Object value) {
        String parameter = metric.getQueryParameter();
        String valueStr = value.toString();

        if (parameters.containsKey(parameter)) {
            throw new IllegalArgumentException("Parameter " + parameter + " is already added");
        }

        parameters.put(parameter, valueStr);
        query = null;
    }

    String getQueryString() {
        if (query == null) {
            query = parameters.entrySet().stream().collect(StringBuilder::new,
                    (sb, e) -> (sb.isEmpty() ? sb : sb.append('&'))
                            .append(e.getKey()).append('=').append(URLEncoder.encode(e.getValue(), UTF_8)),
                    StringBuilder::append).toString();
        }
        return query;
    }
}

