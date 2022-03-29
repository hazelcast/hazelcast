package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.impl.type.HazelcastObjectMarker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowValue implements Serializable, HazelcastObjectMarker {
    private List<Object> values;

    public RowValue() {
        values = new ArrayList<>();
    }

    public RowValue(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(final List<Object> values) {
        this.values = values;
    }
}
