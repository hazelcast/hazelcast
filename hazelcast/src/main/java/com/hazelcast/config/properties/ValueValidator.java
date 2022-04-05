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

package com.hazelcast.config.properties;

/**
 * This interface defines a certain validation logic implementation
 * to test if a given value is expected to be correct or not.
 * <p>
 * All verification is done on property level which means that the
 * configuration overall might still be invalid and needs to be checked
 * by the provider vendor before actually using it.
 * <p>
 * All {@code ValueValidator} implementations need to be fully thread-safe
 * and are recommended to be stateless to prevent any kind of unexpected
 * concurrency issues.
 *
 * @param <T> type of the element to be tested
 */
@FunctionalInterface
public interface ValueValidator<T extends Comparable<T>> {

    /**
     * Validates the given value according to the defined validation logic
     * and throws a ValidationException if configuration does not meet the
     * requirements or logical errors are spotted.
     *
     * @param value the value to be tested
     * @throws ValidationException if validation failed
     */
    void validate(T value) throws ValidationException;

}
