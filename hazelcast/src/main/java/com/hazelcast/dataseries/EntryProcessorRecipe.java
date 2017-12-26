package com.hazelcast.dataseries;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;

public class EntryProcessorRecipe implements DataSerializable {

    private Mutator mutator;
    private Predicate predicate;

    public EntryProcessorRecipe() {
    }

    public EntryProcessorRecipe(Predicate predicate, Mutator mutator) {
        this.predicate = predicate;
        this.mutator = mutator;
    }

    public Mutator getMutator() {
        return mutator;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
        out.writeObject(mutator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        mutator = in.readObject();
    }
}
