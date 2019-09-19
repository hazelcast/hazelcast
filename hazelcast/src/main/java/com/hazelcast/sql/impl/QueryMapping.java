package com.hazelcast.sql.impl;

/**
 * Enumeration which defines query mapping.
 */
public enum QueryMapping {
    /** Fragmant should be executed on all data members. */
    DATA_MEMBERS(0),

    /** Fragment should be executed only on a this member. */
    SINGLETON_LOCAL(1),

    /** Fragment should be executed only on a single remote member. */
    SINGLETON_REMOTE(2),

    /** Fragment should be executed on custom nodes, the list of nodes is provided separately. */
    CUSTOM(3);

    private final int id;

    QueryMapping(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static QueryMapping getById(final int id) {
        for (QueryMapping type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
