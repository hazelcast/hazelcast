package com.hazelcast.sql.impl;

/**
 * Enumeration which defines query mapping.
 */
public enum QueryFragmentMapping {
    /** Fragmant should be executed on all data members. */
    DATA_MEMBERS(0),

    /** Fragment should be executed only on a root member. */
    ROOT(1),

    /** Fragment should be executed only on a single random data member. */
    REPLICATED(2);

    private final int id;

    QueryFragmentMapping(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static QueryFragmentMapping getById(final int id) {
        for (QueryFragmentMapping type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
