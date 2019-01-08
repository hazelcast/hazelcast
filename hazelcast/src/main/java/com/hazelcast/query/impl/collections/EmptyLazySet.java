package com.hazelcast.query.impl.collections;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class EmptyLazySet<E> extends LazySet<E> {

    public static <E> LazySet<E> create() {
        return new EmptyLazySet<E>();
    }

    @Nonnull
    @Override
    protected Set<E> initialize() {
        return Collections.emptySet();
    }

    @Override
    public int estimatedSize() {
        return 0;
    }
}
