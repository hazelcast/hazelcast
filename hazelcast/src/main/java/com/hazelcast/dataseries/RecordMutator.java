package com.hazelcast.dataseries;

public interface RecordMutator<E> extends Mutator<E> {

    boolean mutate(E object);
}
