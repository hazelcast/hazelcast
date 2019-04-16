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

package com.hazelcast.datastream;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.datastream.impl.DSService;
import com.hazelcast.datastream.impl.aggregation.ExecuteAggregationOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;

public class PreparedAggregation<E> {

    private final OperationService operationService;
    private final String preparedId;
    private final String name;

    public PreparedAggregation(OperationService operationService,
                               String name,
                               String preparedId) {
        this.operationService = operationService;
        this.name = name;
        this.preparedId = preparedId;
    }

    /**
     * Executes the whole aggregation on the partition thread (can be very problematic with large datastream)
     *
     * @param bindings
     * @return
     */
    public E executePartitionThread(Map<String, Object> bindings) {
        return execute(bindings, false);
    }

    /**
     * Executes the aggregation on forjoin pool. Only the eden region is processed on partition thread, but
     * all the tenured region will be processed on the forkjoin pool.
     *
     * @param bindings
     * @return
     */
    public E executeForkJoin(Map<String, Object> bindings) {
        return execute(bindings, true);
    }

    private E execute(Map<String, Object> bindings, boolean forkJoin) {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new ExecuteAggregationOperationFactory(name, preparedId, bindings, forkJoin));

            Aggregator resultAggregator = null;
            for (Object v : result.values()) {
                Aggregator a = (Aggregator) v;
                if (resultAggregator == null) {
                    resultAggregator = a;
                } else {
                    resultAggregator.combine(a);
                }
            }

            resultAggregator.onCombinationFinished();
            return (E) resultAggregator.aggregate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
