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

package com.hazelcast.jet;

/**
 * Traverses over a sequence of non-{@code null} items, then starts returning
 * {@code null}. Each invocation of {@code next()} consumes and returns the next
 * item in the sequence, until it is exhausted. All subsequent invocations of
 * {@code next()} return {@code null}.
 *
 * @param <T> traversed item type
 */
@FunctionalInterface
public interface Traverser<T> {
    /**
     * @return the next item in the sequence, or {@code null} if the sequence is
     * already exhausted
     */
    T next();
}
