/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.impl;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Contract point for tracking count of stored cache entries.
 */
public abstract class CacheEntryCountResolver {

    public abstract long getEntryCount();
    public abstract void setEntryCount(long count);

    public abstract long increaseEntryCount();
    public abstract long increaseEntryCount(long count);

    public abstract long decreaseEntryCount();
    public abstract long decreaseEntryCount(long count);

    public static CacheEntryCountResolver createEntryCountResolver() {
        return new DefaultEntryCountResolver();
    }

    public static CacheEntryCountResolver createEntryCountResolver(CacheContext cacheContext) {
        return new CacheContextBackedEntryCountResolver(cacheContext);
    }

    private static class DefaultEntryCountResolver extends CacheEntryCountResolver {

        private final AtomicLong entryCount = new AtomicLong(0L);

        @Override
        public long getEntryCount() {
            return entryCount.get();
        }

        @Override
        public void setEntryCount(long count) {
            entryCount.set(count);
        }

        @Override
        public long increaseEntryCount() {
            return entryCount.incrementAndGet();
        }

        @Override
        public long increaseEntryCount(long count) {
            return entryCount.addAndGet(count);
        }

        @Override
        public long decreaseEntryCount() {
            return entryCount.decrementAndGet();
        }

        @Override
        public long decreaseEntryCount(long count) {
            return entryCount.addAndGet(-count);
        }

    }

    private static class CacheContextBackedEntryCountResolver extends CacheEntryCountResolver {

        private final CacheContext cacheContext;

        public CacheContextBackedEntryCountResolver(CacheContext cacheContext) {
            this.cacheContext = cacheContext;
        }

        @Override
        public long getEntryCount() {
            return cacheContext.getEntryCount();
        }

        @Override
        public void setEntryCount(long count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long increaseEntryCount() {
            return cacheContext.increaseEntryCount();
        }

        @Override
        public long increaseEntryCount(long count) {
            return cacheContext.increaseEntryCount(count);
        }

        @Override
        public long decreaseEntryCount() {
            return cacheContext.decreaseEntryCount();
        }

        @Override
        public long decreaseEntryCount(long count) {
            return cacheContext.decreaseEntryCount(count);
        }

    }

}
