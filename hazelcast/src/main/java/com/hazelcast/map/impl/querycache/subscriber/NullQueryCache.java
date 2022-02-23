/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Object for neutral {@code InternalQueryCache} implementation.
 */
@SuppressWarnings("checkstyle:methodcount")
public final class NullQueryCache implements InternalQueryCache {

    /**
     * Null query cache implementation.
     */
    public static final InternalQueryCache NULL_QUERY_CACHE = new NullQueryCache();

    private NullQueryCache() {
    }

    @Override
    public void set(Object key, Object value, EntryEventType eventType) {
    }

    @Override
    public void prepopulate(Iterator entries) {
    }

    @Override
    public void delete(Object key, EntryEventType eventType) {
    }

    @Override
    public int removeEntriesOf(int partitionId) {
        return 0;
    }

    @Override
    public IMap getDelegate() {
        return null;
    }

    @Override
    public void clear() {
    }

    @Override
    public void setPublisherListenerId(UUID publisherListenerId) {

    }

    @Override
    public UUID getPublisherListenerId() {
        return null;
    }

    @Override
    public String getCacheId() {
        return null;
    }

    @Override
    public boolean reachedMaxCapacity() {
        return false;
    }

    @Override
    public Extractors getExtractors() {
        return null;
    }

    @Override
    public void recreate() {

    }

    @Override
    public Object get(Object key) {
        return null;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void addIndex(IndexConfig config) {
        // No-op.
    }

    @Override
    public Map getAll(Set keys) {
        return null;
    }

    @Override
    public Set keySet() {
        return null;
    }

    @Override
    public Set keySet(Predicate predicate) {
        return null;
    }

    @Override
    public Set<Map.Entry> entrySet() {
        return null;
    }

    @Override
    public Set<Map.Entry> entrySet(Predicate predicate) {
        return null;
    }

    @Override
    public Collection values() {
        return null;
    }

    @Override
    public Collection values(Predicate predicate) {
        return null;
    }

    @Override
    public UUID addEntryListener(MapListener listener, boolean includeValue) {
        return null;
    }

    @Override
    public UUID addEntryListener(MapListener listener, Object key, boolean includeValue) {
        return null;
    }

    @Override
    public UUID addEntryListener(MapListener listener, Predicate predicate, boolean includeValue) {
        return null;
    }

    @Override
    public UUID addEntryListener(MapListener listener, Predicate predicate, Object key, boolean includeValue) {
        return null;
    }

    @Override
    public boolean removeEntryListener(UUID id) {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean tryRecover() {
        return false;
    }

    @Override
    public void destroy() {
    }
}
