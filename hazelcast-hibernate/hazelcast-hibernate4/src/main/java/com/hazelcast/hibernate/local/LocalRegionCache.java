/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.local;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.hibernate.HazelcastTimestamper;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.Value;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.access.SoftLock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local only {@link com.hazelcast.hibernate.RegionCache} implementation
 * based on a topic to distribute cache updates.
 */
public class LocalRegionCache implements RegionCache {

    private static final long SEC_TO_MS = 1000L;
    private static final int MAX_SIZE = 100000;
    private static final float BASE_EVICTION_RATE = 0.2F;

    protected final HazelcastInstance hazelcastInstance;
    protected final ITopic<Object> topic;
    protected final MessageListener<Object> messageListener;
    protected final ConcurrentMap<Object, Expirable> cache;
    protected final Comparator versionComparator;
    protected final AtomicLong markerIdCounter;
    protected MapConfig config;

    /**
     * @param name              the name for this region cache, which is also used to retrieve configuration/topic
     * @param hazelcastInstance the {@code HazelcastInstance} to which this region cache belongs, used to retrieve
     *                          configuration and to lookup an {@link ITopic} to register a {@link MessageListener}
     *                          with (optional)
     * @param metadata          metadata describing the cached data, used to compare data versions (optional)
     */
    public LocalRegionCache(final String name, final HazelcastInstance hazelcastInstance,
                            final CacheDataDescription metadata) {
        this(name, hazelcastInstance, metadata, true);
    }

    /**
     * @param name              the name for this region cache, which is also used to retrieve configuration/topic
     * @param hazelcastInstance the {@code HazelcastInstance} to which this region cache belongs, used to retrieve
     *                          configuration and to lookup an {@link ITopic} to register a {@link MessageListener}
     *                          with if {@code withTopic} is {@code true} (optional)
     * @param metadata          metadata describing the cached data, used to compare data versions (optional)
     * @param withTopic         {@code true} to register a {@link MessageListener} with the {@link ITopic} whose name
     *                          matches this region cache <i>if</i> a {@code HazelcastInstance} was provided to look
     *                          up the topic; otherwise, {@code false} not to register a listener even if an instance
     *                          was provided
     * @since 3.3
     */
    public LocalRegionCache(final String name, final HazelcastInstance hazelcastInstance,
                            final CacheDataDescription metadata, final boolean withTopic) {
        this.hazelcastInstance = hazelcastInstance;
        try {
            config = hazelcastInstance != null ? hazelcastInstance.getConfig().findMapConfig(name) : null;
        } catch (UnsupportedOperationException e) {
            Logger.getLogger(LocalRegionCache.class).finest(e);
        }
        versionComparator = metadata != null && metadata.isVersioned() ? metadata.getVersionComparator() : null;
        cache = new ConcurrentHashMap<Object, Expirable>();
        markerIdCounter = new AtomicLong();

        messageListener = createMessageListener();
        if (withTopic && hazelcastInstance != null) {
            topic = hazelcastInstance.getTopic(name);
            topic.addMessageListener(messageListener);
        } else {
            topic = null;
        }
    }

    public Object get(final Object key, long txTimestamp) {
        final Expirable value = cache.get(key);
        return value != null ? value.getValue(txTimestamp) : null;
    }

    @Override
    public boolean insert(final Object key, final Object value, final Object currentVersion) {
        final Value newValue = new Value(currentVersion, nextTimestamp(), value);
        return cache.putIfAbsent(key, newValue) == null;
    }

    public boolean put(final Object key, final Object value, final long txTimestamp, final Object version) {
        while (true) {
            Expirable previous = cache.get(key);
            Value newValue = new Value(version, nextTimestamp(), value);
            if (previous == null) {
                if (cache.putIfAbsent(key, newValue) == null) {
                    return true;
                }
            } else if (previous.isReplaceableBy(txTimestamp, version, versionComparator)) {
                if (cache.replace(key, previous, newValue)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public boolean update(final Object key, final Object newValue, final Object newVersion, final SoftLock softLock) {
        boolean updated;
        while (true) {
            Expirable original = cache.get(key);
            Expirable revised;
            long timestamp = nextTimestamp();
            if (original == null) {
                // The entry must have expired. it should be safe to update
                revised = new Value(newVersion, timestamp, newValue);
                updated = true;
                if (cache.putIfAbsent(key, revised) == null) {
                    break;
                }
            } else {
                if (original.matches(softLock)) {
                    // The lock matches
                    final ExpiryMarker marker = (ExpiryMarker) original;
                    if (marker.isConcurrent()) {
                        revised = marker.expire(timestamp);
                        updated = false;
                    } else {
                        revised = new Value(newVersion, timestamp, newValue);
                        updated = true;
                    }
                    if (cache.replace(key, original, revised)) {
                        break;
                    }
                } else if (original.getValue() == null) {
                    // It's marked for expiration, leave it as is
                    updated = false;
                    break;
                } else {
                    // It's a value. Instead of removing it, expire it to prevent stale from in progress
                    // transactions being put in the cache
                    revised = new ExpiryMarker(newVersion, timestamp, nextMarkerId()).expire(timestamp);
                    updated = false;
                    if (cache.replace(key, original, revised)) {
                        break;
                    }
                }
            }
        }
        maybeNotifyTopic(key, newValue, newVersion);

        return updated;
    }

    protected void maybeNotifyTopic(final Object key, final Object value, final Object version) {
        if (topic != null) {
            topic.publish(createMessage(key, value, version));
        }
    }

    protected Object createMessage(final Object key, final Object value, final Object currentVersion) {
        return new Invalidation(key, currentVersion);
    }

    protected MessageListener<Object> createMessageListener() {
        return new MessageListener<Object>() {
            public void onMessage(final Message<Object> message) {
                final Invalidation invalidation = (Invalidation) message.getMessageObject();
                if (versionComparator != null) {
                    final Expirable value = cache.get(invalidation.getKey());
                    if (value != null) {
                        Object currentVersion = value.getVersion();
                        Object newVersion = invalidation.getVersion();
                        if (versionComparator.compare(newVersion, currentVersion) > 0) {
                            cache.remove(invalidation.getKey(), value);
                        }
                    }
                } else {
                    cache.remove(invalidation.getKey());
                }
            }
        };
    }

    public boolean remove(final Object key) {
        final Expirable value = cache.remove(key);
        if (value != null) {
            maybeNotifyTopic(key, null, value.getVersion());
            return true;
        }
        return false;
    }

    public SoftLock tryLock(final Object key, final Object version) {
        ExpiryMarker marker;
        String markerId = nextMarkerId();
        while (true) {
            final Expirable original = cache.get(key);
            long timeout = nextTimestamp() + CacheEnvironment.getDefaultCacheTimeoutInMillis();
            if (original == null) {
                marker = new ExpiryMarker(version, timeout, markerId);
                if (cache.putIfAbsent(key, marker) == null) {
                    break;
                }
            } else {
                marker = original.markForExpiration(timeout, markerId);
                if (cache.replace(key, original, marker)) {
                    break;
                }
            }
        }
        return marker;
    }

    public void unlock(final Object key, SoftLock lock) {
        while (true) {
            final Expirable original = cache.get(key);
            if (original != null) {
                if (original.matches(lock)) {
                    final Expirable revised = ((ExpiryMarker) original).expire(nextTimestamp());
                    if (cache.replace(key, original, revised)) {
                        break;
                    }
                } else if (original.getValue() != null) {
                    if (cache.remove(key, original)) {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    public boolean contains(final Object key) {
        return cache.containsKey(key);
    }

    public void clear() {
        cache.clear();
    }

    public long size() {
        return cache.size();
    }

    public long getSizeInMemory() {
        return 0;
    }

    public Map asMap() {
        return cache;
    }

    void cleanup() {
        final int maxSize;
        final long timeToLive;
        if (config != null) {
            maxSize = config.getMaxSizeConfig().getSize();
            timeToLive = config.getTimeToLiveSeconds() * SEC_TO_MS;
        } else {
            maxSize = MAX_SIZE;
            timeToLive = CacheEnvironment.getDefaultCacheTimeoutInMillis();
        }

        boolean limitSize = maxSize > 0 && maxSize != Integer.MAX_VALUE;
        if (limitSize || timeToLive > 0) {
            List<EvictionEntry> entries = searchEvictableEntries(timeToLive, limitSize);
            final int diff = cache.size() - maxSize;
            final int evictionRate = calculateEvictionRate(diff, maxSize);
            if (evictionRate > 0 && entries != null) {
                evictEntries(entries, evictionRate);
            }
        }
    }

    private String nextMarkerId() {
        return Long.toString(markerIdCounter.getAndIncrement());
    }

    private long nextTimestamp() {
        return hazelcastInstance == null ? Clock.currentTimeMillis()
                : HazelcastTimestamper.nextTimestamp(hazelcastInstance);
    }

    private List<EvictionEntry> searchEvictableEntries(long timeToLive, boolean limitSize) {
        List<EvictionEntry> entries = null;
        Iterator<Entry<Object, Expirable>> iter = cache.entrySet().iterator();
        long now = nextTimestamp();
        while (iter.hasNext()) {
            final Entry<Object, Expirable> e = iter.next();
            final Object k = e.getKey();
            final Expirable expirable = e.getValue();
            if (expirable instanceof ExpiryMarker) {
                continue;
            }
            final Value v = (Value) expirable;
            if (v.getTimestamp() + timeToLive < now) {
                iter.remove();
            } else if (limitSize) {
                if (entries == null) {
                    // Use a List rather than a Set for correctness. Using a Set, especially a TreeSet
                    // based on EvictionEntry.compareTo, causes evictions to be processed incorrectly
                    // when two or more entries in the map have the same timestamp. In such a case, the
                    // _first_ entry at a given timestamp is the only one that can be evicted because
                    // TreeSet does not add "equivalent" entries. A second benefit of using a List is
                    // that the cost of sorting the entries is not incurred if eviction isn't performed
                    entries = new ArrayList<EvictionEntry>(cache.size());
                }
                entries.add(new EvictionEntry(k, v));
            }
        }
        return entries;
    }

    private int calculateEvictionRate(int diff, int maxSize) {
        return diff >= 0 ? (diff + (int) (maxSize * BASE_EVICTION_RATE)) : 0;
    }

    private void evictEntries(List<EvictionEntry> entries, int evictionRate) {
        // Only sort the entries if we're going to evict some
        Collections.sort(entries);
        int removed = 0;
        for (EvictionEntry entry : entries) {
            if (cache.remove(entry.key, entry.value) && ++removed == evictionRate) {
                break;
            }
        }
    }

    /**
     * Inner class that instances represent an entry marked for eviction
     */
    private static final class EvictionEntry implements Comparable<EvictionEntry> {
        final Object key;
        final Value value;

        private EvictionEntry(final Object key, final Value value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(final EvictionEntry o) {
            final long thisVal = this.value.getTimestamp();
            final long anotherVal = o.value.getTimestamp();
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            EvictionEntry that = (EvictionEntry) o;

            return (key == null ? that.key == null : key.equals(that.key))
                    && (value == null ? that.value == null : value.equals(that.value));
        }

        @Override
        public int hashCode() {
            return key == null ? 0 : key.hashCode();
        }
    }
}
