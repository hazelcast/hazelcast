package com.hazelcast.query.impl;

import com.hazelcast.query.impl.TypeConverters.TypeConverter;

/**
 * Converts to the same value
 **/
public class IdentityConverter implements TypeConverter {

    @Override
    public Comparable convert(final Comparable value) {
        return value;
    }

}
