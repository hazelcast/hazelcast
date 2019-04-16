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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @param <In>
 * @param <Out>
 */
public class AggregationRecipe<In, Out> implements DataSerializable {

    private String projectionClassName;
    private String aggregatorClassName;
    private Predicate predicate;
    private Map<String, Object> parameters;

    public AggregationRecipe() {
    }

    public AggregationRecipe(Class projectionClass, Aggregator aggregator, Predicate predicate) {
        this.projectionClassName = projectionClass.getName();
        this.aggregatorClassName = aggregator.getClass().getName();
        this.predicate = predicate;
        this.parameters = new HashMap<>();
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public AggregationRecipe addParam(String name, Object value) {
        parameters.put(name, value);
        return this;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public String getProjectionClassName() {
        return projectionClassName;
    }

    public String getAggregatorClassName() {
        return aggregatorClassName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(projectionClassName);
        out.writeObject(predicate);
        out.writeUTF(aggregatorClassName);
        out.writeInt(parameters.size());
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.projectionClassName = in.readUTF();
        this.predicate = in.readObject();
        this.aggregatorClassName = in.readUTF();

        int size = in.readInt();
        this.parameters = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            parameters.put(in.readUTF(), in.readObject());
        }
    }
}
