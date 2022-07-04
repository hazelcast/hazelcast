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

package com.hazelcast.aws;

/**
 * Represents tag key and value pair. Used to narrow the members returned by the discovery mechanism.
 */
class Tag {
    private final String key;
    private final String value;

    Tag(String key, String value) {
        if (key == null && value == null) {
            throw new IllegalArgumentException("Tag requires at least key or value");
        }
        this.key = key;
        this.value = value;
    }

    String getKey() {
        return key;
    }

    String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("(key=%s, value=%s)", key, value);
    }
}
