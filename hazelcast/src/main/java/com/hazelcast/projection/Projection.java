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

package com.hazelcast.projection;

import java.io.Serializable;

/**
 * Enables transforming object into other objects.
 * Exemplary usage scenario is the project() method of the {@link com.hazelcast.core.IMap}
 * <p>
 * Only 1:1 transformations allowed. Use an Aggregator to perform N:1 or N:M aggregations.
 * <p>
 * <pre>{@code
 *      IMap<String, Employee> employees = instance.getMap("employees");
 *
 *      Collection<String> names = employees.project(new Projection<Map.Entry<String,Employee>,String>(){
 *          @Override
 *          public String transform(Map.Entry<String, Employee>entry){
 *              return entry.getValue().getName();
 *          }
 *      });
 * }</pre>
 *
 * @param <I> input type
 * @param <O> output type
 * @since 3.8
 */
public abstract class Projection<I, O> implements Serializable {

    /**
     * Transforms the input object into the output object.
     *
     * @param input object.
     * @return the output object.
     */
    public abstract O transform(I input);

}
