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

package com.hazelcast.projection;

import com.hazelcast.map.IMap;

import java.io.Serializable;

/**
 * Enables transforming object into other objects.
 * Exemplary usage scenario is the project() method of the {@link IMap}
 * <p>
 * Only 1:1 transformations allowed. Use an Aggregator to perform N:1 or N:M aggregations.
 * <pre>
 *      IMap&lt;String, Employee&gt; employees = instance.getMap("employees");
 *
 *      Collection&lt;String&gt; names = employees.project(new Projection&lt;Map.Entry&lt;String,Employee&gt;,String&gt;(){
 *          &#64;Override
 *          public String transform(Map.Entry&lt;String, Employee&gt; entry){
 *              return entry.getValue().getName();
 *          }
 *      });
 * </pre>
 *
 * @param <I> input type
 * @param <O> output type
 * @since 3.8
 */
@FunctionalInterface
public interface Projection<I, O> extends Serializable {

    /**
     * Transforms the input object into the output object.
     *
     * @param input object.
     * @return the output object.
     */
    O transform(I input);

}
