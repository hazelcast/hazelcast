/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore.expiry;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpirySystemTest {

    @Mock
    private RecordStore recordStore;
    @Mock
    protected MapContainer mapContainer;
    @Mock
    private MapServiceContext mapServiceContext;
    @Mock
    protected NodeEngine nodeEngine;
    @Mock
    private ExpirationManager expirationManager;
    @Mock
    private HazelcastProperties hazelcastProperties;

    @Mock
    private MapClearExpiredRecordsTask mapClearExpiredRecordsTask;
    @Mock
    private Storage storage;
    @Mock
    private ILogger logger;

    private ExpirySystem expirySystem;

    @Before
    public void setUp() {
        when(mapServiceContext.getExpirationManager()).thenReturn(expirationManager);
        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(logger);
        when(nodeEngine.getProperties()).thenReturn(hazelcastProperties);
        when(hazelcastProperties.getMillis(ClusterProperty.MAP_EXPIRY_DELAY_SECONDS)).thenReturn(10L);
        when(hazelcastProperties.getNanos(any())).thenReturn(1L);
        when(mapServiceContext.getClearExpiredRecordsTask()).thenReturn(mapClearExpiredRecordsTask);
        lenient().when(recordStore.getStorage()).thenReturn(storage);
        expirySystem = createExpirySystem(recordStore, mapContainer, mapServiceContext);
    }

    @Test
    public void testAddWithExpiryMetadata() {
        Data key = setupKeyAndMockStorage();
        long ttl = 1000;
        long maxIdle = 2000;
        long expirationTime = 3000;
        long lastUpdateTime = 4000;

        ExpiryMetadata expiryMetadata = createExpiryMetadata(ttl, maxIdle, expirationTime, lastUpdateTime);
        expirySystem.add(key, expiryMetadata, System.currentTimeMillis());
        ExpiryMetadata storedMetadata = expirySystem.getExpiryMetadata(key);
        assertExpiryMetadata(storedMetadata, ttl, maxIdle, expirationTime, lastUpdateTime);
        verify(expirationManager).scheduleExpirationTask();
    }

    @Test
    public void testAddWithExpiry() {
        Data key = setupKeyAndMockStorage();
        long ttl = 1000;
        long maxIdle = 2000;
        long expirationTime = 3000;
        long lastUpdateTime = 4000;

        expirySystem.add(key, ttl, maxIdle, expirationTime, lastUpdateTime, System.currentTimeMillis());
        ExpiryMetadata storedMetadata = expirySystem.getExpiryMetadata(key);
        assertExpiryMetadata(storedMetadata, ttl, maxIdle, expirationTime, lastUpdateTime);
        verify(expirationManager).scheduleExpirationTask();
    }

    @Test
    public void testAddWhenTTLDeterminedByMapConfig() {
        Data key = setupKeyAndMockStorage();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setTimeToLiveSeconds(5);
        long now = 10000;
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);

        expirySystem.add(key, now, now);
        ExpiryMetadata storedMetadata = expirySystem.getExpiryMetadata(key);
        assertExpiryMetadata(storedMetadata, 5000, Long.MAX_VALUE, now + 5000, now);
        verify(expirationManager).scheduleExpirationTask();
    }

    @Test
    public void testAddWhenTTLIsZeroDoesNotStoreForExpiration() {
        Data key = setupKeyAndMockStorage();
        MapConfig mapConfig = new MapConfig();
        long now = 10000;
        long ttl = 0;
        long maxIdle = -1;
        long expirationTime = -1;
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);

        expirySystem.add(key, ttl, maxIdle, expirationTime, now, now);
        ExpiryMetadata storedMetadata = expirySystem.getExpiryMetadata(key);
        assertNotNull(storedMetadata);
        assertEquals(ExpiryMetadata.NULL, storedMetadata);
        verify(expirationManager, never()).scheduleExpirationTask();
    }

    @Test
    public void testAddWhenTTLIsUnsetDoesNotStoreForExpiration() {
        Data key = setupKeyAndMockStorage();
        MapConfig mapConfig = new MapConfig();
        long now = 10000;
        long ttl = -1;
        long maxIdle = -1;
        long expirationTime = -1;
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);

        expirySystem.add(key, ttl, maxIdle, expirationTime, now, now);
        ExpiryMetadata storedMetadata = expirySystem.getExpiryMetadata(key);
        assertNotNull(storedMetadata);
        assertEquals(ExpiryMetadata.NULL, storedMetadata);
        verify(expirationManager, never()).scheduleExpirationTask();
    }

    @Test
    public void testAddWhenNoTtlOrExpiryTimeSetDoesNotStoreForExpiration() {
        Data key = setupKeyAndMockStorage();
        long now = 10000;
        long ttl = Long.MAX_VALUE;
        long maxIdle = -1;
        long expirationTime = -1;
        when(mapContainer.getMapConfig()).thenReturn(new MapConfig());

        expirySystem.add(key, ttl, maxIdle, expirationTime, now, now);
        ExpiryMetadata storedMetadata = expirySystem.getExpiryMetadata(key);
        assertNotNull(storedMetadata);
        assertEquals(ExpiryMetadata.NULL, storedMetadata);
        verify(expirationManager, never()).scheduleExpirationTask();
    }

    private Data setupKeyAndMockStorage() {
        Data key = new HeapData(new byte[10]);
        lenient().when(storage.toBackingDataKeyFormat(key)).thenReturn(key);
        return key;
    }

    private void assertExpiryMetadata(ExpiryMetadata storedMetadata, long ttl, long maxIdle, long expirationTime, long lastUpdateTime) {
        assertNotNull(storedMetadata);
        assertEquals(ttl, storedMetadata.getTtl());
        assertEquals(maxIdle, storedMetadata.getMaxIdle());
        assertEquals(expirationTime, storedMetadata.getExpirationTime());
        assertEquals(lastUpdateTime, storedMetadata.getLastUpdateTime());
    }

    protected ExpiryMetadata createExpiryMetadata(long ttl, long maxIdle,
                                                  long expirationTime,
                                                  long lastUpdateTime) {
        return new ExpiryMetadataImpl(ttl, maxIdle, expirationTime, lastUpdateTime);
    }

    protected ExpirySystem createExpirySystem(RecordStore recordStore,
                                               MapContainer mapContainer,
                                               MapServiceContext mapServiceContext) {
        return new ExpirySystemImpl(recordStore, mapContainer, mapServiceContext);
    }
}
