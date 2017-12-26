package com.hazelcast.dataseries;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;

public class ProjectionRecipe<E> implements DataSerializable {

    private String className;
    private boolean reusePojo;
    private Predicate predicate;

    public ProjectionRecipe() {
    }

    public ProjectionRecipe(Class<E> clazz, boolean reusePojo, Predicate predicate) {
        this(clazz.getName(), reusePojo, predicate);
    }

    public ProjectionRecipe(String className, boolean reusePojo, Predicate predicate) {
        this.className = className;
        this.reusePojo = reusePojo;
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public String getClassName() {
        return className;
    }

    public boolean isReusePojo() {
        return reusePojo;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(className);
        out.writeBoolean(reusePojo);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readUTF();
        reusePojo = in.readBoolean();
        predicate = in.readObject();
    }
}
