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

package com.hazelcast.jet.io.tuple;

/**
 * A factory of tuples.
 */
public interface TupleFactory {
    /**
     * Returns a new 2-tuple (a pair) with the given components.
     */
     <T0, T1> Tuple2<T0, T1> tuple2(T0 c0, T1 c1);

    /**
     * Returns a new tuple with the components provided in the argument array.
     */
     Tuple tuple(Object[] components);
}
