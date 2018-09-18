package com.hazelcast.internal.probing;

/**
 * Can be implemented by {@link Enum}s to allow semi-automatic conversion from
 * {@link Enum} constant to a code metric.
 */
public interface CodedEnum {

    /**
     * @return a stable unique code within the set of all enum constants of the same
     *         enum that does not change over time.
     */
    int getCode();
}
