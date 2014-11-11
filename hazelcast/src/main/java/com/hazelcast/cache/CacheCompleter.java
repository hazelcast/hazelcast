package com.hazelcast.cache;

/**
 * TODO add a proper JavaDoc
 */
public interface CacheCompleter {

    void countDownCompletionLatch(int id);
}
