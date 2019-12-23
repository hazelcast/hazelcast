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

package com.hazelcast.config;

import com.hazelcast.internal.util.StringUtil;

/**
 * Defines an assortment of transforms which can be applied to {@link
 * IndexConfig#getUniqueKey() unique key} values.
 * <p>
 * Currently, applicable only to bitmap indexes.
 */
public enum UniqueKeyTransform {

    /**
     * Extracted unique key value is interpreted as an object value.
     * Non-negative unique ID is assigned to every distinct object value.
     */
    OBJECT("OBJECT", 0),

    /**
     * Extracted unique key value is interpreted as a whole integer value of
     * byte, short, int or long type. The extracted value is upcasted to
     * long (if necessary) and unique non-negative ID is assigned to every
     * distinct value.
     */
    LONG("LONG", 1),

    /**
     * Extracted unique key value is interpreted as a whole integer value of
     * byte, short, int or long type. The extracted value is upcasted to
     * long (if necessary) and the resulting value is used directly as an ID.
     */
    RAW("RAW", 2);

    private final String name;
    private final int id;

    UniqueKeyTransform(String name, int id) {
        this.name = name;
        this.id = id;
    }

    /**
     * Resolves one of the {@link UniqueKeyTransform} values by its name.
     *
     * @param name the name of the {@link UniqueKeyTransform} value to resolve.
     * @return the resolved {@link UniqueKeyTransform} value.
     * @throws IllegalArgumentException if the given name doesn't correspond
     *                                  to any known {@link UniqueKeyTransform}
     *                                  name.
     */
    public static UniqueKeyTransform fromName(String name) {
        if (StringUtil.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("empty unique key transform");
        }

        String upperCasedText = name.toUpperCase();
        if (upperCasedText.equals(OBJECT.name)) {
            return OBJECT;
        }
        if (upperCasedText.equals(LONG.name)) {
            return LONG;
        }
        if (upperCasedText.equals(RAW.name)) {
            return RAW;
        }

        throw new IllegalArgumentException("unexpected unique key transform: " + name);
    }

    static UniqueKeyTransform fromId(int id) {
        for (UniqueKeyTransform transform : values()) {
            if (transform.id == id) {
                return transform;
            }
        }

        throw new IllegalArgumentException("unexpected unique key transform id: " + id);
    }

    /**
     * @return the id of this transform.
     */
    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return name;
    }

}
