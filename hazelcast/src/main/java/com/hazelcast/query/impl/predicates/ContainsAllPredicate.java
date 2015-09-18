package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.collection.ArrayUtils;
import com.hazelcast.util.collection.InflatableSet;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.isNotNull;

public final class ContainsAllPredicate extends AbstractPredicate implements Predicate {

    private Set<Comparable> values;

    ContainsAllPredicate(String attribute, Set<Comparable> values) {
        super(attribute);
        isNotNull(values, "Values cannot be null");
        this.values = values;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return null;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        for (Comparable value : values) {
            Object o = readAttribute(mapEntry);
            if (o instanceof Object[]) {
                Object[] attributes = (Object[]) o;
                boolean valueFound = ArrayUtils.contains(attributes, value);
                if (!valueFound) {
                    return false;
                }
            }
        }
        return true;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(values.size());
        for (Comparable c : values) {
            out.writeObject(c);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        int size = in.readInt();
        InflatableSet.Builder<Comparable> builder = InflatableSet.newBuilder(size);
        for (int i = 0; i < size; i++) {
            Comparable o = in.readObject();
            builder.add(o);
        }
        values = builder.build();
    }
}
