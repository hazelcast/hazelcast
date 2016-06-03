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

package com.hazelcast.jet.data.tuple;

import com.hazelcast.jet.io.tuple.TupleFactory;
import com.hazelcast.jet.strategy.CalculationStrategy;

/**
 * Factory to create jet-tuple;
 */
public interface JetTupleFactory extends TupleFactory {

    /**
     * Will create tuple with 1-element key part and 1-element value part;
     *
     * @param k                   - value of the key part;
     * @param v                   - value of the value part;
     * @param partitionId         - partitionId;
     * @param calculationStrategy - calculation strategy to be used inside the tuple hash calculation;
     * @param <K>                 - type of the key part;
     * @param <V>                 - value of the key part;
     * @return - constructed tuple;
     */
    <K, V> JetTuple<K, V> tuple(K k, V v, int partitionId, CalculationStrategy calculationStrategy);


    /**
     * Will create tuple with 1-element key part and multi-element value part;
     *
     * @param k                   - value of the key part;
     * @param v-                  value of the value part;
     * @param <K>                 - type of the key part;
     * @param <V>                 - value of the key part;
     * @param partitionId         - partitionId of the data in key part;
     * @param calculationStrategy - calculation strategy to be used inside the tuple hash calculation;
     * @return - constructed tuple;
     */
    <K, V> JetTuple<K, V> tuple(K k, V[] v, int partitionId, CalculationStrategy calculationStrategy);

    /**
     * Will create tuple with multi-element key part and multi-element value part;
     *
     * @param k                   - value of the key part;
     * @param v-                  value of the value part;
     * @param <K>                 - type of the key part;
     * @param <V>                 - value of the key part;
     * @param partitionId         - partitionId of the data in key part;
     * @param calculationStrategy - calculation strategy to be used inside the tuple hash calculation;
     * @return - constructed tuple;
     */
    <K, V> JetTuple<K, V> tuple(K[] k, V[] v, int partitionId, CalculationStrategy calculationStrategy);
}
