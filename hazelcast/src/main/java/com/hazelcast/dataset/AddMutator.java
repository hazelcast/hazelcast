package com.hazelcast.dataset;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class AddMutator<E> implements FieldMutator<E> {

    private Number number;
    private String field;

    public AddMutator() {
    }

    public AddMutator(String field, Number number) {
        this.field = field;
        this.number = number;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(field);
        out.writeObject(number);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        field = in.readUTF();
        number = in.readObject();
    }
}
