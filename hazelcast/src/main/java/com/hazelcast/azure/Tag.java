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

package com.hazelcast.azure;

import com.hazelcast.config.InvalidConfigurationException;

/**
 * Represents a Azure Tag (key and value).
 */
final class Tag {
    private final String key;
    private final String value;

    /**
     * Creates {@link Tag} from a string "key=value".
     */
    Tag(String spec) {
        String[] labelParts = spec.trim().split("\\s*=\\s*");
        if (labelParts.length != 2) {
            throw new InvalidConfigurationException(String.format("Invalid tag specification: '%s'", spec));
        }
        this.key = labelParts[0];
        this.value = labelParts[1];
    }

    Tag(String key, String value) {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tag tag = (Tag) o;

        if (!key.equals(tag.key)) {
            return false;
        }
        return value.equals(tag.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Tag{"
                + "key='" + key + '\''
                + ", value='" + value + '\''
                + '}';
    }
}
