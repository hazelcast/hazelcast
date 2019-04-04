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

package com.hazelcast.internal.util;

/**
 * Provides utilities to work with Java 8+ optionals.
 */
public interface Optionals {

    /**
     * Unwraps the given potentially optional value.
     *
     * @param value the potentially optional value to unwrap.
     * @return an unwrapped value, if the given value is optional; the
     * original given value, if it's not optional.
     */
    Object unwrapIfOptional(Object value);

}
