package com.hazelcast.jet.sql.impl.connector.mongodb;

import org.bson.Document;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

public class DynamicParameter {

    private static final List<String> NODES = asList("index", "objectType");
    private static final String DYNAMIC_PARAMETER_DISCRIMINATOR = "DynamicParameter";

    public final String objectType = DYNAMIC_PARAMETER_DISCRIMINATOR;
    private final int index;

    public DynamicParameter(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public static DynamicParameter parse(Document doc) {
        if (doc.keySet().containsAll(NODES)) {
            Object objectType = doc.get("objectType");
            assert objectType instanceof String;
            if (DYNAMIC_PARAMETER_DISCRIMINATOR.equals(objectType)) {
                return new DynamicParameter(doc.getInteger("index"));
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DynamicParameter)) {
            return false;
        }
        DynamicParameter that = (DynamicParameter) o;
        return index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectType, index);
    }

    @Override
    public String toString() {
        return "ParameterReplace(" + index + ')';
    }
}
