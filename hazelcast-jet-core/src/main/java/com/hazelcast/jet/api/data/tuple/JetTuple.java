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

package com.hazelcast.jet.api.data.tuple;

import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.api.PartitionIdAware;
import com.hazelcast.jet.api.strategy.CalculationStrategy;
import com.hazelcast.jet.api.strategy.CalculationStrategyAware;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

/**
 * Represents abstract tuple of integrated with another JET's abstractions;
 *
 * @param <K> - type of keys;
 * @param <V> - type of value;
 */
public interface JetTuple<K, V> extends CalculationStrategyAware, PartitionIdAware, Tuple<K, V> {
    /**
     * Represents binary representation of key-data;
     *
     * @param nodeEngine - Hazelcast nodeEngine;
     * @return - Hazelcast Data object;
     */
    Data getKeyData(NodeEngine nodeEngine);


    /**
     * Represents binary representation of value-data;
     *
     * @param nodeEngine - Hazelcast nodeEngine;
     * @return - Hazelcast Data object;
     */
    Data getValueData(NodeEngine nodeEngine);

    /**
     * Represents binary representation of key-data;
     * Use calculationStrategy to construct Data;
     *
     * @param calculationStrategy - calculation strategy to be used;
     * @param nodeEngine          - Hazelcast nodeEngine;
     * @return - Hazelcast Data object;
     */
    Data getKeyData(CalculationStrategy calculationStrategy, NodeEngine nodeEngine);

    /**
     * Represents binary representation of value-data;
     * Use calculationStrategy to construct Data;
     *
     * @param calculationStrategy - calculation strategy to be used;
     * @param nodeEngine          - Hazelcast nodeEngine;
     * @return - Hazelcast Data object;
     */
    Data getValueData(CalculationStrategy calculationStrategy, NodeEngine nodeEngine);


    /**
     * Represents binary representation of key-object with specified index;
     *
     * @param index               - corresponding index;
     * @param nodeEngine          - Hazelcast nodeEngine;
     * @param calculationStrategy the calculation strategy to use for the key
     * @return - Hazelcast Data object;
     */
    Data getKeyData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine);


    /**
     * Represents binary representation of value-object with specified index;
     *
     * @param index               - corresponding index;
     * @param nodeEngine          - Hazelcast nodeEngine;
     * @param calculationStrategy the calculation strategy to use for the value
     * @return - Hazelcast Data object;
     */
    Data getValueData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine);
}
