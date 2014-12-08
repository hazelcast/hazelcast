package com.hazelcast.cache.impl;

/**
 * used to state that a sync event completed and any resources waiting for it should be released
 */
public interface CacheSyncListenerCompleter {

    void countDownCompletionLatch(int id);
}
