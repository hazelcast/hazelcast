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

package com.hazelcast.dictionary.impl.type;

/**
 * todo:
 *
 * fixed length string
 *
 * regular string
 */
public enum Kind {

    PRIMITIVE(true),

    /**
     * Long, Double etc.
     */
    PRIMITIVE_WRAPPER(true),

    PRIMITIVE_ARRAY(false),

    UNKNOWN(false),

    FIXED_LENGTH_RECORD(true),

    VARIABLE_LENGTH_RECORD(false),

    STRING(false);

    private final boolean isFixedLength;

    Kind(boolean isFixedLength) {
        this.isFixedLength = isFixedLength;
    }

    /**
     * Checks if this kind required fixed length storage or variable length storage.
     *
     * @return true if fixed length.
     */
    public boolean isFixedLength() {
        return isFixedLength;
    }
}
