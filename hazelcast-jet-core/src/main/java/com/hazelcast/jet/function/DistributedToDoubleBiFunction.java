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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.util.function.ToDoubleBiFunction;

/**
 * Represents a function that accepts two arguments and produces a double-valued
 * result.  This is the {@code double}-producing primitive specialization for
 * {@link DistributedBiFunction}.
 *
 * <p>This is a functional interface
 * whose functional method is {@link #applyAsDouble(Object, Object)}.
 *
 * @param <T> the type of the first argument to the function
 * @param <U> the type of the second argument to the function
 * @see DistributedBiFunction
 */
@FunctionalInterface
public interface DistributedToDoubleBiFunction<T, U> extends ToDoubleBiFunction<T, U>, Serializable {
}
