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

import com.hazelcast.jet.api.strategy.CalculationStrategy;

public class DefaultJetTupleFactory implements JetTupleFactory {
    @Override
    public <K, V> JetTuple<K, V> tuple(K k, V v) {
        return new JetTuple2<K, V>(k, v);
    }

    @Override
    public <K, V> JetTuple<K, V> tuple(K k, V v, int partitionID, CalculationStrategy calculationStrategy) {
        return new JetTuple2<K, V>(k, v, partitionID, calculationStrategy);
    }

    @Override
    public <K, V> JetTuple<K, V> tuple(K k, V[] v) {
        return new DefaultJetTuple<K, V>(k, v);
    }

    @Override
    public <K, V> JetTuple<K, V> tuple(K k, V[] v, int partitionId, CalculationStrategy calculationStrategy) {
        return new DefaultJetTuple<K, V>(k, v, partitionId, calculationStrategy);
    }

    @Override
    public <K, V> JetTuple<K, V> tuple(K[] k, V[] v) {
        return new DefaultJetTuple<K, V>(k, v);
    }

    @Override
    public <K, V> JetTuple<K, V> tuple(K[] k, V[] v, int partitionID, CalculationStrategy calculationStrategy) {
        return new DefaultJetTuple<K, V>(k, v, partitionID, calculationStrategy);
    }
}
