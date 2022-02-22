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

package com.hazelcast.test.backup;

import com.hazelcast.core.HazelcastInstance;

import javax.cache.expiry.ExpiryPolicy;

import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertEqualsStringFormat;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.backup.CacheBackupAccessor.NON_EXISTENT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Convenience utility for accessing and asserting backup records.
 */
public final class TestBackupUtils {

    private static final int DEFAULT_REPLICA_INDEX = 1;

    private TestBackupUtils() {
    }

    /**
     * Creates a new instance of {@link BackupAccessor} for a given map.
     * <p>
     * It access a first backup replica.
     *
     * @param cluster all instances in the cluster
     * @param mapName map to access
     * @param <K>     type of keys
     * @param <V>     type of values
     * @return accessor for a given map and first backup replica
     */
    public static <K, V> BackupAccessor<K, V> newMapAccessor(HazelcastInstance[] cluster, String mapName) {
        return newMapAccessor(cluster, mapName, DEFAULT_REPLICA_INDEX);
    }

    /**
     * Creates a new instance of {@link BackupAccessor} for a given map.
     * <p>
     * It allows to access an arbitrary replica index as long as it's greater or equals {@code 1}.
     *
     * @param cluster      all instances in the cluster
     * @param mapName      map to access
     * @param replicaIndex replica index to access
     * @param <K>          type of keys
     * @param <V>          type of values
     * @return accessor for a given map and replica index
     */
    public static <K, V> BackupAccessor<K, V> newMapAccessor(HazelcastInstance[] cluster, String mapName, int replicaIndex) {
        return new MapBackupAccessor<>(cluster, mapName, replicaIndex);
    }

    /**
     * Creates a new instance of {@link BackupAccessor} for a given cache.
     * <p>
     * It access a first backup replica.
     *
     * @param cluster   all instances in the cluster
     * @param cacheName cache to access
     * @param <K>       type of keys
     * @param <V>       type of values
     * @return accessor for a given cache and first backup replica
     */
    public static <K, V> BackupAccessor<K, V> newCacheAccessor(HazelcastInstance[] cluster, String cacheName) {
        return newCacheAccessor(cluster, cacheName, DEFAULT_REPLICA_INDEX);
    }

    /**
     * Creates a new instance of {@link BackupAccessor} for a given cache.
     * <p>
     * It allows to access an arbitrary replica index as long as it's greater or equals {@code 1}.
     *
     * @param cluster      all instances in the cluster
     * @param cacheName    cache to access
     * @param replicaIndex replica index to access
     * @param <K>          type of keys
     * @param <V>          type of values
     * @return accessor for a given cache and replica index
     */
    public static <K, V> BackupAccessor<K, V> newCacheAccessor(HazelcastInstance[] cluster, String cacheName, int replicaIndex) {
        return new CacheBackupAccessor<>(cluster, cacheName, replicaIndex);
    }

    public static <K, V> void assertBackupEntryEqualsEventually(final K key, final V expectedValue,
                                                                final BackupAccessor<K, V> accessor) {
        assertTrueEventually(() -> {
            V actualValue = accessor.get(key);
            assertEqualsStringFormat("Expected backup entry with key '" + key + "' to be '%s', but was '%s'",
                    expectedValue, actualValue);
        });
    }

    public static <K, V> void assertBackupEntryNullEventually(final K key, final BackupAccessor<K, V> accessor) {
        assertTrueEventually(() -> {
            V actualValue = accessor.get(key);
            assertNull("Expected backup entry with key '" + key + "' to be null, but was '" + actualValue + "'", actualValue);
        });
    }

    public static <K, V> void assertBackupSizeEventually(final int expectedSize, final BackupAccessor<K, V> accessor) {
        assertTrueEventually(() -> assertEqualsStringFormat("Expected size %d, but was %d", expectedSize, accessor.size()));
    }

    public static <K, V> void assertExpiryPolicyEventually(final K key, final ExpiryPolicy expiryPolicy,
                                                           final BackupAccessor<K, V> accessor) {
        checkInstanceOf(CacheBackupAccessor.class, accessor, "Need to supply a CacheBackupAccessor");
        assertTrueEventually(() -> {
            CacheBackupAccessor<K, V> cacheBackupAccessor = (CacheBackupAccessor<K, V>) accessor;
            assertEquals(expiryPolicy, cacheBackupAccessor.getExpiryPolicy(key));
        });
    }

    public static <K, V> void assertExpirationTimeExistsEventually(final K key, final BackupAccessor<K, V> accessor) {
        checkInstanceOf(CacheBackupAccessor.class, accessor, "Need to supply a CacheBackupAccessor");
        assertTrueEventually(() -> {
            CacheBackupAccessor<K, V> cacheBackupAccessor = (CacheBackupAccessor<K, V>) accessor;
            long expirationTime = cacheBackupAccessor.getExpirationTime(key);

            String msg;
            if (expirationTime == NON_EXISTENT_KEY) {
                msg = String.format("Non existent key on backup partition key=%s, accessor=%s", key, accessor);
            } else {
                // expirationTime is set to Duration.ETERNAL if no expiry policy is defined.
                msg = String.format("key=%s, expirationTime=%d, accessor=%s", key, expirationTime, accessor);
            }

            assertTrue(msg, expirationTime > 0 && expirationTime < Long.MAX_VALUE);
        });
    }
}
