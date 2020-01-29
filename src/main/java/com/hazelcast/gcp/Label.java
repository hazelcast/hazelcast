/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.gcp;

/**
 * Represents a GCP Label (key and value).
 */
final class Label {
    private final String key;
    private final String value;

    /**
     * Creates {@link Label} from a string "key=value".
     */
    Label(String spec) {
        String[] labelParts = spec.trim().split("\\s*=\\s*");
        if (labelParts.length != 2) {
            throw new IllegalArgumentException(String.format("Invalid label specification: '%s'", spec));
        }
        this.key = labelParts[0];
        this.value = labelParts[1];
    }

    String getKey() {
        return key;
    }

    String getValue() {
        return value;
    }
}
