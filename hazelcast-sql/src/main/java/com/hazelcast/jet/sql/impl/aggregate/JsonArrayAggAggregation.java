package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class JsonArrayAggAggregation implements SqlAggregation {
    private List<Object> objects = new ArrayList<>();


    public static JsonArrayAggAggregation create() {
        return new JsonArrayAggAggregation();
    }

    @Override
    public void accumulate(Object value) {
        objects.add(value);
    }

    @Override
    public void combine(SqlAggregation other) {
        JsonArrayAggAggregation otherC = (JsonArrayAggAggregation) other;
        objects.addAll(otherC.objects);
    }

    @Override
    public Object collect() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (objects.size() > 0) {
            sb.append(objects.get(0).toString());
        }
        for (int i = 1; i < objects.size(); i++) {
            Object o = objects.get(i);
            if (o == null) {
                sb.append(", ");
                sb.append("null");
                continue;
            }
            sb.append(", ");
            sb.append(objects.get(i).toString());
        }
        sb.append("]");

        return new HazelcastJsonValue(sb.toString());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(objects.size());
        for (Object o : objects) {
            out.writeObject(o);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            objects.add(in.readObject());
        }
    }
}
