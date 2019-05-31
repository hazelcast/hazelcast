/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.map.impl.mapstore.EntryStoreSimpleTest;
import com.hazelcast.map.impl.mapstore.TestEntryStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryStoreWriteBehindTest extends EntryStoreSimpleTest {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig("default").getMapStoreConfig().setWriteDelaySeconds(1);
        return config;
    }

    @Override
    protected void assertEntryStore(String key, String value) {
        assertTrueEventually(() -> {
            super.assertEntryStore(key, value);
        }, 10);
    }

    @Override
    protected void assertEntryStore(String key, String value, long remainingTtl, TimeUnit timeUnit, long delta) {
        long expectedExpirationTime = System.currentTimeMillis() + timeUnit.toMillis(remainingTtl);
        assertTrueEventually(() -> {
        TestEntryStore.Record record = testEntryStore.getRecord(key);
        assertNotNull(record);
        assertEquals(value, record.value);
            assertBetween("expirationTime", record.expirationTime, expectedExpirationTime - delta, expectedExpirationTime + delta);

        }, 10);
    }
}
