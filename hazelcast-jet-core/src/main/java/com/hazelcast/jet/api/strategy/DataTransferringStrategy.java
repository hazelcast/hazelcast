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

package com.hazelcast.jet.api.strategy;

import java.io.Serializable;

/**
 * Represents strategy of data transferring;
 *
 * Jet uses ringBuffers to pass data from one vertex to another;
 *
 * RingBuffers can work by reference or by value;
 *
 * By references means that all input objects will be copied into the
 * ringBuffer by reference (without internal copy);
 *
 * By value means ringBuffer has pre-allocated objects; Data will be copied
 * from input objects into pre-allocated objects;
 *
 * Used depends on the flow to avoid copy or in opposite to avoid garbage creation;
 *
 * @param <T> - type if input source objects to be written into the ringBuffer;
 */
public interface DataTransferringStrategy<T> extends Serializable {
    /**
     * @return true - in case of "by reference" strategy, false in case if "by value" strategy;
     */
    boolean byReference();

    /**
     * Creates new instance of object;
     *
     * @return the new instance
     */
    T newInstance();

    /**
     * Copy data from one object to another;
     *
     * @param sourceObject - object with source data;
     * @param targetObject - target object;
     */
    void copy(T sourceObject, T targetObject);

    /**
     * Clean object's internals;
     *
     * @param object - object to clean;
     */
    void clean(T object);
}
