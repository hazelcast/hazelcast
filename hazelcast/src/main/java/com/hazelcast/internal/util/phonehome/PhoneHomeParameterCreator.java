/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

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
     * {@code value}.
     *
     * @param key the parameter key
     * @param value the parameter value
     */
    public void addParam(String key, String value) {
        if (parameters.containsKey(key)) {
            throw new IllegalArgumentException("Parameter " + key + " is already added");
        }

        if (hasParameterBefore) {
            builder.append("&");
        } else {
            hasParameterBefore = true;
        }
        try {
            builder.append(key).append("=").append(URLEncoder.encode(value, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw rethrow(e);
        }
        parameters.put(key, value);
    }

    String build() {
        return builder.toString();
    }
}

