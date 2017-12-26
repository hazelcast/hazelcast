package com.hazelcast.dataseries;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
        projectionClassName = in.readUTF();
        predicate = in.readObject();
        aggregatorClassName = in.readUTF();

        int size = in.readInt();
        parameters = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            parameters.put(in.readUTF(), in.readObject());
        }
    }
}
