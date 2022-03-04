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

package com.hazelcast.core;

/**
 * Implementations of this interface define a certain type conversion.
 * Conversion can happen from any kind of {@link java.lang.Comparable}
 * type to another.
 * <p>
 * Implementations of TypeConverter need to be fully thread-safe and
 * must have no internal state as they are expected to be used by
 * multiple threads and with shared instances.
 */
@FunctionalInterface
public interface TypeConverter {

    /**
     * Compares a {@link java.lang.Comparable}-typed value to another.
     * Since TypeConverters are not statically typed themselves, the developer
     * needs to take care of correct usage of input and output types.
     *
     * @param value the value to be converted
     * @return the converted value
     */
    Comparable convert(Comparable value);

}
