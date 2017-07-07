package com.hazelcast.internal.util.iterator;

import com.hazelcast.core.IFunction;

import java.util.Iterator;

/**
 * Invoke operation on each member provided by the member iterator.
 *
 */
public class MappingIterator<I, O> implements Iterator<O> {
    private final Iterator<I> memberIterator;
    private final IFunction<I, O> mappingFunction;

    public MappingIterator(Iterator<I> memberIterator, IFunction<I, O> mappingFunction) {
        this.memberIterator = memberIterator;
        this.mappingFunction = mappingFunction;
    }

    @Override
    public boolean hasNext() {
        return memberIterator.hasNext();
    }

    @Override
    public O next() {
        I element = memberIterator.next();
        return mappingFunction.apply(element);
    }

    @Override
    public void remove() {
        memberIterator.remove();
    }
}
