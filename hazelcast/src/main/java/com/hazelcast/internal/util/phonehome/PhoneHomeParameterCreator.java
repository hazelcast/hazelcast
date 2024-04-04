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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Util class for parameters of OS and EE PhoneHome pings.
 */
public class PhoneHomeParameterCreator {
    private final StringBuilder builder;
    private final Map<String, String> parameters = new HashMap<>();
    private boolean hasParameterBefore;

    public PhoneHomeParameterCreator() {
        builder = new StringBuilder();
    }

    Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Adds a parameter with the provided {@code key} and the associated
     * {@code value}. The key and value will be converted to string.
     */
    public void addParam(@Nonnull Object key, @Nonnull Object value) {
        String keyStr = key.toString();
        String valueStr = value.toString();

        if (parameters.containsKey(keyStr)) {
            throw new IllegalArgumentException("Parameter " + keyStr + " is already added");
        }

        if (hasParameterBefore) {
            builder.append("&");
        } else {
            hasParameterBefore = true;
        }
        builder.append(keyStr).append("=").append(URLEncoder.encode(valueStr, StandardCharsets.UTF_8));
        parameters.put(keyStr, valueStr);
    }

    String build() {
        return builder.toString();
    }
}

