package com.hazelcast.raft.impl.util;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Pair<X, Y> {

    private final X primary;
    private final Y secondary;

    public Pair(X primary, Y secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }

    public X getPrimary() {
        return primary;
    }

    public Y getSecondary() {
        return secondary;
    }
}
