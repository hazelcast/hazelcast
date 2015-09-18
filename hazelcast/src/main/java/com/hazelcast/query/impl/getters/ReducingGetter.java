package com.hazelcast.query.impl.getters;

public class ReducingGetter extends Getter {
    private final int position;

    public ReducingGetter(Getter parent, String suffix) {
        super(parent);
        String stringValue = suffix.substring(1, suffix.length() - 1);
        position = Integer.parseInt(stringValue);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        Object[] value = (Object[]) parent.getValue(obj);
        if (value == null || value.length - 1 < position) {
            return null;
        }
        return value[position];
    }

    @Override
    Class getReturnType() {
        return parent.getReturnType();
    }

    @Override
    boolean isCacheable() {
        return parent.isCacheable();
    }
}
