/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast;

import com.google.common.base.Strings;

import java.io.Serializable;
import java.util.stream.IntStream;

public class IdentifiedDataSerializableObject implements Serializable {
    private static final int MIN_LENGTH = 10;
    private static final int RANGE = 20;
    private final String value;
    private final Integer[] integers;

    public IdentifiedDataSerializableObject(String value, Integer[] integers) {
        this.value = value;
        this.integers = integers;
    }

    public static IdentifiedDataSerializableObject of(Integer integer) {
        String value = Strings.padStart(integer.toString(), MIN_LENGTH, '0');
        Integer[] integers = IntStream.range(integer, integer + RANGE)
                .boxed()
                .toArray(Integer[]::new);
        return new IdentifiedDataSerializableObject(value, integers);
    }

    public String getValue() {
        return value;
    }
}
