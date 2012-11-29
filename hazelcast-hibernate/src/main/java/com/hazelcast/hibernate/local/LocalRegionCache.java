/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.util.Clock;
import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.access.SoftLock;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 11/9/12
 */
public class LocalRegionCache implements RegionCache {

    protected final ITopic<Object> topic;
    protected final MessageListener<Object> messageListener;
    protected final ConcurrentMap<Object, Value> cache;
    protected final Comparator versionComparator;
    protected MapConfig config;

    public LocalRegionCache(final String name, final HazelcastInstance hazelcastInstance,
                            final CacheDataDescription metadata) {
        try {
            config = hazelcastInstance != null ? hazelcastInstance.getConfig().findMatchingMapConfig(name) : null;
        } catch (UnsupportedOperationException ignored) {
        }
        versionComparator = metadata != null && metadata.isVersioned() ? metadata.getVersionComparator() : null;
        cache = new ConcurrentHashMap<Object, Value>();

        messageListener = createMessageListener();
        if (hazelcastInstance != null) {
            topic = hazelcastInstance.getTopic(name);
            topic.addMessageListener(messageListener);
        } else {
            topic = null;
        }
    }

    public Object get(final Object key) {
        final Value value = cache.get(key);
        return value != null ? value.getValue() : null;
    }

    public boolean put(final Object key, final Object value, final Object currentVersion,
                       final Object previousVersion, final SoftLock lock) {

//        System.out.println(key + ", " + value + ": " + currentVersion + "/" + previousVersion + " -> " + lock);
        if (lock == LOCK_FAILURE) {
//            System.out.println("LOCK failure ???");
            return false;
        }

        final Value currentValue = cache.get(key);
        if (lock == LOCK_SUCCESS) {
            if (currentValue != null && currentVersion != null
                && versionComparator.compare(currentVersion, currentValue.getVersion()) < 0) {
//                System.out.println("Lock current version mismatch !!! ");
                return false;
            }
        }
        if (topic != null) {
            topic.publish(createMessage(key, value, currentVersion));
        }
        cache.put(key, new Value(currentVersion, value, lock, Clock.currentTimeMillis()));
        return true;
    }

    protected Object createMessage(final Object key, Object value, final Object currentVersion) {
        return new Invalidation(key, currentVersion);
    }

    protected MessageListener<Object> createMessageListener() {
        return new MessageListener<Object>() {
            public void onMessage(final Message<Object> message) {
                final Invalidation invalidation = (Invalidation) message.getMessageObject();
//        System.out.println("invalidation = " + invalidation);
                if (versionComparator != null) {
                    final Value value = cache.get(invalidation.getKey());
                    if (value != null) {
                        Object currentVersion = value.getVersion();
                        Object newVersion = invalidation.getVersion();
//                System.out.println("currentVersion = " + currentVersion + " -> newVersion = " + newVersion);
                        if (versionComparator.compare(newVersion, currentVersion) > 0) {
//                    System.out.println("Invalidating... " + invalidation);
                            cache.remove(invalidation.getKey(), value);
                        }
                    }
                } else {
//            System.out.println("Invalidating XXX ... " + invalidation);
                    cache.remove(invalidation.getKey());
                }
            }
        };
    }

    public boolean remove(final Object key) {
        return cache.remove(key) != null;
    }

    public SoftLock tryLock(final Object key, final Object version) {
        final Value value = cache.get(key);
        if (value == null) {
            if (cache.putIfAbsent(key, new Value(version, null, LOCK_SUCCESS, Clock.currentTimeMillis())) == null) {
//                System.out.println("locked = " + LOCK_SUCCESS);
                return LOCK_SUCCESS;
            } else {
//                System.out.println("put if absent failed !!!");
                return LOCK_FAILURE;
            }
        } else {
            if (version == null || versionComparator.compare(version, value.getVersion()) >= 0) {
                if (cache.replace(key, value, value.createLockedValue(LOCK_SUCCESS))) {
//                    System.out.println("locked = " + LOCK_SUCCESS);
                    return LOCK_SUCCESS;
                } else {
//                    System.out.println("replace failed !!!");
                    return LOCK_FAILURE;
                }
            } else {
//                System.out.println("version mismatch !!!");
                return LOCK_FAILURE;
            }
        }
    }

    public void unlock(final Object key, SoftLock lock) {
        final Value value = cache.get(key);
        if (value != null) {
            final SoftLock currentLock = value.getLock();
            if (currentLock == lock) {
                if (cache.replace(key, value, value.createUnlockedValue())) {
//                    System.out.println("UNlocked ----");
                } else {
//                    System.out.println("Unlock replace fail !!!");
                }
            } else {
//                System.out.println("Lock not found !!!");
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

    public void onMessage(final Message<Invalidation> message) {
        final Invalidation invalidation = message.getMessageObject();
//        System.out.println("invalidation = " + invalidation);
        if (versionComparator != null) {
            final Value value = cache.get(invalidation.getKey());
            if (value != null) {
                Object currentVersion = value.getVersion();
                Object newVersion = invalidation.getVersion();
//                System.out.println("currentVersion = " + currentVersion + " -> newVersion = " + newVersion);
                if (versionComparator.compare(newVersion, currentVersion) > 0) {
//                    System.out.println("Invalidating... " + invalidation);
                    cache.remove(invalidation.getKey(), value);
                }
            }
        } else {
//            System.out.println("Invalidating XXX ... " + invalidation);
            cache.remove(invalidation.getKey());
        }
    }

    void cleanup() {
        final int maxSize;
        final long timeToLive;
        if (config != null) {
            maxSize = config.getMaxSizeConfig().getSize();
            timeToLive = config.getTimeToLiveSeconds() * 1000L;
        } else {
            maxSize = 100000;
            timeToLive = CacheEnvironment.getDefaultCacheTimeoutInMillis();
        }

        if ((maxSize > 0 && maxSize != Integer.MAX_VALUE) || timeToLive > 0) {
            final Iterator<Entry<Object, Value>> iter = cache.entrySet().iterator();
            SortedSet<EvictionEntry> entries = null;
            final long now = Clock.currentTimeMillis();
            while (iter.hasNext()) {
                final Entry<Object, Value> e = iter.next();
                final Object k = e.getKey();
                final Value v = e.getValue();
                if (v.getLock() == LOCK_SUCCESS) {
                    continue;
                }
                if (v.getCreationTime() + timeToLive > now) {
                    iter.remove();
                } else if (maxSize > 0 && maxSize != Integer.MAX_VALUE) {
                    if (entries == null) {
                        entries = new TreeSet<EvictionEntry>();
                    }
                    entries.add(new EvictionEntry(k, v));
                }
            }
            final int diff = cache.size() - maxSize;
            final int k = diff >= 0 ? (diff + maxSize * 20 / 100) : 0;
            if (k > 0 && entries != null) {
                int i = 0;
                for (EvictionEntry entry : entries) {
                    if (cache.remove(entry.key, entry.value)) {
                        if (++i == k) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private class EvictionEntry implements Comparable<EvictionEntry> {
        final Object key;
        final Value value;

        private EvictionEntry(final Object key, final Value value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(final EvictionEntry o) {
            final long thisVal = this.value.getCreationTime();
            final long anotherVal = o.value.getCreationTime();
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
        }
    }

    private static final SoftLock LOCK_SUCCESS = new SoftLock() {
        @Override
        public String toString() {
            return "Lock::Success";
        }
    };

    private static final SoftLock LOCK_FAILURE = new SoftLock() {
        @Override
        public String toString() {
            return "Lock::Failure";
        }
    };

}
