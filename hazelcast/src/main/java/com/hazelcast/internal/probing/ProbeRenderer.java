package com.hazelcast.internal.probing;

public interface ProbeRenderer {

    /**
     * Appends the given key value pair to the output in the format specific to the
     * renderer.
     * 
     * @param key immutable, not null
     * @param value -1 for unknown, >= 0 otherwise (double encoded as 10000 x double
     *        value)
     */
    void render(CharSequence key, long value);

}
