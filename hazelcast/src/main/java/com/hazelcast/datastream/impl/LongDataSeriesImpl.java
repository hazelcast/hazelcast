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

package com.hazelcast.datastream.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.datastream.AggregationRecipe;
import com.hazelcast.datastream.LongDataSeries;
import com.hazelcast.datastream.PreparedAggregation;
import com.hazelcast.datastream.impl.aggregation.PrepareAggregationOperationFactory;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.OperationService;

import java.util.HashMap;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;

public class LongDataSeriesImpl implements LongDataSeries {

    private final String fieldName;
    private final String dataStreamName;
    private final OperationService operationService;
    private final String maxRecipe;
    private final String minRecipe;
    private final String sumRecipe;
    private final String averageRecipe;
    private volatile boolean useForkJoin = true;

    LongDataSeriesImpl(String fieldName, OperationService operationService, String dataStreamName) {
        this.fieldName = fieldName;
        this.operationService = operationService;
        this.dataStreamName = dataStreamName;
        this.maxRecipe = newRecipe(new MaxAggregator());
        this.minRecipe = newRecipe(new MinAggregator());
        this.sumRecipe = newRecipe(new LongSumAggregator());
        this.averageRecipe = newRecipe(new LongAverageAggregator());
    }

    @Override
    public void useForkJoin(boolean useForkJoin) {
        this.useForkJoin = useForkJoin;
    }

    private String newRecipe(Aggregator aggregator) {
        String preparationId = newPreparationId();
        AggregationRecipe recipe = new AggregationRecipe(LongHolder.class, aggregator, new SqlPredicate("true"));
        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME,
                    new PrepareAggregationOperationFactory(dataStreamName, preparationId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return preparationId;
    }

    private String newPreparationId() {
        return dataStreamName + "_" + fieldName + "_" + newUnsecureUuidString().replace("-", "");
    }

    @Override
    public String fieldName() {
        return null;
    }

    @Override
    public long min() {
        return (Long) execute(minRecipe);
    }

    @Override
    public long max() {
        return (Long) execute(maxRecipe);
    }


    @Override
    public long sum() {
        return (Long) execute(sumRecipe);
    }

    @Override
    public double average() {
        return (Double) execute(averageRecipe);
    }

    private Number execute(String preparationId) {
        PreparedAggregation<Long> aggregation = new PreparedAggregation<>(
                operationService,
                dataStreamName,
                preparationId);
        HashMap<String, Object> bindings = new HashMap<>();
        return useForkJoin
                ? aggregation.executeForkJoin(bindings)
                : aggregation.executePartitionThread(bindings);
    }
}
