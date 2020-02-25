/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.config;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.util.StringUtil;

public class LowercaseHandler {

    private Map<String, String> lowersMap;

    public LowercaseHandler() {
        this.lowersMap = new HashMap<>();
    }

    public void put(String value, boolean shouldLowercase) {
        if (value != null) {
            lowersMap.put(shouldLowercase ? StringUtil.lowerCaseInternal(value) : value, value);
        }
    }

    public String get(String value) {
        return lowersMap.get(value);
    }

}
