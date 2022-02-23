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

package com.hazelcast.internal.serialization;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicates that the annotated class cannot be converted to {@link IdentifiedDataSerializable} due to other conventions.
 * The annotation's {@link #value()} provides reasoning why class cannot be converted. Classes annotated with
 * {@code SerializableByConvention} are excluded from compliance tests by {@code DataSerializableConventionsTest}.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface SerializableByConvention {
    Reason value() default Reason.INHERITANCE;

    enum Reason {
        /**
         * Class definition is part of public API so cannot be migrated to {@link IdentifiedDataSerializable} in
         * minor or patch releases.
         */
        PUBLIC_API,
        /**
         * Class is {@code Serializable} due to inheritance or other coding conventions and cannot or is not desirable
         * to have it converted to {@link IdentifiedDataSerializable}.
         * Examples:
         * <ul>
         *     <li>inheritance from a {@code Serializable} class external to Hazelcast e.g.
         *     {@link javax.cache.event.CacheEntryEvent} is serializable however its implementation in
         *     {@link com.hazelcast.cache.impl.CacheEntryEventImpl} is never used in serialized form, so there
         *     is no interest in converting this class to {@link IdentifiedDataSerializable}</li>
         *     <li>a {@code Comparator} which should by convention also implement {@code Serializable}.</li>
         * </ul>
         *
         * Classes whose superclasses are {@link java.io.Serializable} without a default constructor or with private
         * final fields, such as {@link Throwable} or {@link java.util.EventObject}, cannot be converted to
         * {@link IdentifiedDataSerializable}, so these are not annotated with {@code SerializableByConvention}.
         * Instead these classes are white-listed in
         * {@code com.hazelcast.internal.serialization.impl.DataSerializableConventionsTest#SERIALIZABLE_WHITE_LIST}
         * and are excluded from conventions testing.
         */
        INHERITANCE,
    }
}
