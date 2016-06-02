/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.internal.api.actor;

/**
 * This is an abstract interface for each consumer
 *
 * @param <T> - type of the consumed object
 */
public interface Consumer<T> {
    /**
     * Method to consume an abstract entry
     *
     * @param entry - entity to consume
     * @return true if entry has been consumed
     * false if entry hasn't been consumed
     * @throws Exception if any exception
     */
    boolean consume(T entry) throws Exception;
}
