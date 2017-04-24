/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

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
         * Class is {@code Serializable} due to conventions and cannot be converted to {@code IdentifiedDataSerializable}.
         * Examples:
         * <ul>
         *     <li>inheritance from a class external to Hazelcast e.g. {@link javax.cache.event.CacheEntryEvent} imposes
         *     serializability to its implementation in {@link com.hazelcast.cache.impl.CacheEntryEventImpl} even though
         *     the latter is never serialized</li>
         *     <li>inheritance from a class external to Hazelcast, however {@link IdentifiedDataSerializable} requires a
         *     default no-args constructor and non-final fields which is not always possible (e.g. subclasses of
         *     {@code java.util.EventObject} & {@code java.lang.Exception})</li>
         *     <li>a {@code Comparator} which should by convention also implement {@code Serializable}.</li>
         * </ul>
         */
        INHERITANCE,
    }
}
