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

package com.hazelcast.jet.io.spi.tuple;

/**
 * Factory to create tuple;
 */
public interface TupleFactory {
    /**
     * Will create tuple with 1-element key part and 1-element value part;
     *
     * @param k   - value of the key part;
     * @param v-  value of the value part;
     * @param <K> - type of the key part;
     * @param <V> - value of the key part;
     * @return - constructed tuple;
     */
    <K, V> Tuple<K, V> tuple(K k, V v);


    /**
     * Will create tuple with 1-element key part and multi-element value part
     *
     * @param k   - value of the key part
     * @param v-  values of the value part
     * @param <K> - type of the key part
     * @param <V> - value of the key part
     * @return - constructed tuple;
     */
    <K, V> Tuple<K, V> tuple(K k, V[] v);


    /**
     * Will create tuple with multi-element key part and multi-element value part;
     *
     * @param k   - values of the key part;
     * @param v-  values of the value part;
     * @param <K> - type of the key part;
     * @param <V> - value of the key part;
     * @return - constructed tuple;
     */
    <K, V> Tuple<K, V> tuple(K[] k, V[] v);
}
