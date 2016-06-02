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

package com.hazelcast.jet.api.processor;

import com.hazelcast.jet.api.dag.Vertex;

/**
 * Represents abstract factory to construct JET user-level container processors;
 * Note: each object which will be constructed by factory will be assigned to the separate container task;
 * <p/>
 * So if for example it return always the same instance of the ContainerProcessor - the same instance object
 * can be executed from different threads. It can be useful in some cases;
 *
 * @param <I> - type of the input objects;
 * @param <O> - type of the output objects;
 */
public interface ContainerProcessorFactory<I, O> {
    /**
     * Constructs and returns container processor to be used in task execution;
     *
     * @param vertex - corresponding vertex;
     * @return - corresponding processor;
     */
    ContainerProcessor<I, O> getProcessor(Vertex vertex);
}
