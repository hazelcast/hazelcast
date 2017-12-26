package com.hazelcast.dataseries;

/**
 * Mutates a single field of a record.
 *
 * @param <E>
 */
public interface FieldMutator<E> extends Mutator<E> {

    String getField();
}
