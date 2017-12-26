package com.hazelcast.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;

public class AggregationRecipe<In, Out> implements DataSerializable {

    private String projectionClassName;
    private String aggregatorClassName;
    private Predicate predicate;

    public AggregationRecipe() {
    }

    public AggregationRecipe(Class projectionClass, Aggregator aggregator, Predicate predicate) {
        this.projectionClassName = projectionClass.getName();
        this.aggregatorClassName = aggregator.getClass().getName();
        this.predicate = predicate;
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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        projectionClassName = in.readUTF();
        predicate = in.readObject();
        aggregatorClassName = in.readUTF();
    }
}
