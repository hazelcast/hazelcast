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

import com.hazelcast.internal.serialization.BinaryInterface;

import java.io.Serializable;

/**
 * Represents a function that accepts one argument and produces a result.
 * <p>
 * This class is called IFunction instead of Function to prevent clashes with the one in Java 8.
 * <p>
 * Serialized instances of this interface are used in client-member communication, so changing an implementation's binary format
 * will render it incompatible with its previous versions.
 *
 * @param <T> an argument
 * @param <R> a result
 * @since 3.2
 */
@BinaryInterface
@FunctionalInterface
public interface IFunction<T, R> extends Serializable {

    R apply(T input);
}
