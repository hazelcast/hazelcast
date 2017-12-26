package com.hazelcast.dataset;

public interface RecordMutator<E> extends Mutator<E> {

    boolean mutate(E object);
}
