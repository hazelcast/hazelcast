package com.hazelcast.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public interface AsyncQueue<E> extends IQueue<E> {

    CompletionFuture<Boolean> asyncAdd(E e);

    CompletionFuture<Boolean> asyncOffer(E e);

    CompletionFuture<Void> asyncPut(E e);

    CompletionFuture<Boolean> asyncOffer(E e, long timeout, TimeUnit unit);

    CompletionFuture<E> asyncTake();

    CompletionFuture<E> asyncPoll(long timeout, TimeUnit unit);

    CompletionFuture<Integer> asyncRemainingCapacity();

    CompletionFuture<Boolean> asyncRemove(Object o);

    CompletionFuture<Boolean> asyncContains(Object o);

    CompletionFuture<Integer> asyncDrainTo(Collection<? super E> c);

    CompletionFuture<Integer> asyncDrainTo(Collection<? super E> c, int maxElements);

    CompletionFuture<E> asyncRemove();

    CompletionFuture<E> asyncPoll();

    CompletionFuture<E> asyncElement();

    CompletionFuture<E> asyncPeek();

    CompletionFuture<Integer> asyncSize();

    CompletionFuture<Boolean> asyncIsEmpty();

    CompletionFuture<Iterator<E>> asyncIterator();

    CompletionFuture<Object[]> asyncToArray();

    <T> CompletionFuture<T[]> asyncToArray(T[] a);

    CompletionFuture<Boolean> asyncContainsAll(Collection<?> c);

    CompletionFuture<Boolean> asyncAddAll(Collection<? extends E> c);

    CompletionFuture<Boolean> asyncRemoveAll(Collection<?> c);

    CompletionFuture<Boolean> asyncRetainAll(Collection<?> c);

    CompletionFuture<Void> asyncClear();
}
