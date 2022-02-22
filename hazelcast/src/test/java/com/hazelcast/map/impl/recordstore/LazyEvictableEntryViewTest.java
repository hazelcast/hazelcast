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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.EntryCostEstimatorTest;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.OwnedEntryCostEstimatorFactory;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LazyEvictableEntryViewTest {

    private final int key = 1;
    private final long value = 10L;

    private Record<Data> recordInstance;
    private LazyEvictableEntryView view;
    private EntryCostEstimator costEstimator;

    @Before
    public void setUp() throws Exception {
        view = createLazyEvictableEntryView();
        costEstimator = OwnedEntryCostEstimatorFactory.createMapSizeEstimator(InMemoryFormat.BINARY);
    }

    /**
     * Returns an entry-view instance populated with default values of fields.
     */
    private LazyEvictableEntryView createLazyEvictableEntryView() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setPerEntryStatsEnabled(true);
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        MapContainer mapContainer = mock(MapContainer.class);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);
        DataRecordFactory recordFactory = new DataRecordFactory(mapContainer, serializationService);
        Data key = serializationService.toData(this.key);
        recordInstance = recordFactory.newRecord(key, value);
        return new LazyEvictableEntryView(key, recordInstance, ExpiryMetadata.NULL, serializationService);
    }

    @Test
    public void test_getKey() {
        assertEquals(key, view.getKey());
    }

    @Test
    public void test_getValue() {
        assertEquals(value, view.getValue());
    }

    @Test
    public void test_getCost() {
        assertEquals(EntryCostEstimatorTest.ENTRY_COST_IN_BYTES_WHEN_STATS_ON,
                costEstimator.calculateEntryCost(view.getDataKey(), view.getRecord()));
    }

    @Test
    public void test_getCreationTime() throws Exception {
        assertEquals(0, view.getCreationTime());
    }

    @Test
    public void test_getExpirationTime() throws Exception {
        assertEquals(Long.MAX_VALUE, view.getExpirationTime());
    }

    @Test
    public void test_getHits() throws Exception {
        assertEquals(0, view.getHits());
    }

    @Test
    public void test_getLastAccessTime() throws Exception {
        assertEquals(0, view.getLastAccessTime());
    }

    @Test
    public void test_getLastStoredTime() throws Exception {
        assertEquals(0, view.getLastStoredTime());
    }

    @Test
    public void test_getLastUpdateTime() throws Exception {
        assertEquals(0, view.getLastUpdateTime());
    }

    @Test
    public void test_getVersion() throws Exception {
        assertEquals(0, view.getVersion());
    }

    @Test
    public void test_getTtl() throws Exception {
        assertEquals(Long.MAX_VALUE, view.getTtl());
    }

    @Test
    public void test_getRecord() throws Exception {
        assertEquals(recordInstance, ((LazyEvictableEntryView) view).getRecord());
    }

    @Test
    public void test_equals() throws Exception {
        EntryView entryView = createLazyEvictableEntryView();

        assertTrue(view.equals(entryView) && entryView.equals(view));
    }

    @Test
    public void test_equals_whenSameReference() throws Exception {
        assertTrue(view.equals(view));
    }

    @Test
    public void test_equals_whenSuppliedObjectIsNotEntryView() throws Exception {
        assertFalse(view.equals(this));
    }

    @Test
    public void test_hashCode() throws Exception {
        EntryView entryView = createLazyEvictableEntryView();

        assertEquals(entryView.hashCode(), view.hashCode());
    }

    @Test
    public void test_toString() throws Exception {
        String expected = "EntryView{key=" + view.getKey()
                + ", value=" + view.getValue()
                + ", cost=" + view.getCost()
                + ", version=" + view.getVersion()
                + ", creationTime=" + view.getCreationTime()
                + ", expirationTime=" + view.getExpirationTime()
                + ", hits=" + view.getHits()
                + ", lastAccessTime=" + view.getLastAccessTime()
                + ", lastStoredTime=" + view.getLastStoredTime()
                + ", lastUpdateTime=" + view.getLastUpdateTime()
                + ", ttl=" + view.getTtl()
                + '}';

        assertEquals(expected, view.toString());
    }
}
