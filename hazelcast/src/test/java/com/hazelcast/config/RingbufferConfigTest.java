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

package com.hazelcast.config;

import com.hazelcast.internal.config.RingbufferConfigReadOnly;
import com.hazelcast.internal.config.RingbufferStoreConfigReadOnly;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.RingbufferConfig.DEFAULT_ASYNC_BACKUP_COUNT;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_CAPACITY;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_SYNC_BACKUP_COUNT;
import static com.hazelcast.internal.partition.InternalPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferConfigTest {

    private static final String NAME = "someRingbuffer";

    @Test
    public void testDefaultSetting() {
        RingbufferConfig config = new RingbufferConfig(NAME);
        assertEquals(NAME, config.getName());
        assertEquals(DEFAULT_SYNC_BACKUP_COUNT, config.getBackupCount());
        assertEquals(DEFAULT_ASYNC_BACKUP_COUNT, config.getAsyncBackupCount());
        assertEquals(DEFAULT_CAPACITY, config.getCapacity());
        assertNotNull(config.getMergePolicyConfig());
    }

    @Test
    public void testCloneConstructor() {
        RingbufferConfig original = new RingbufferConfig(NAME);
        original.setBackupCount(2).setAsyncBackupCount(1).setCapacity(10);

        RingbufferConfig clone = new RingbufferConfig(original);

        assertEquals(original.getName(), clone.getName());
        assertEquals(original.getBackupCount(), clone.getBackupCount());
        assertEquals(original.getAsyncBackupCount(), clone.getAsyncBackupCount());
        assertEquals(original.getCapacity(), clone.getCapacity());
    }

    @Test
    public void testCloneConstructorWithName() {
        RingbufferConfig original = new RingbufferConfig(NAME);
        original.setBackupCount(2).setAsyncBackupCount(1).setCapacity(10);

        RingbufferConfig clone = new RingbufferConfig("foobar", original);

        assertEquals("foobar", clone.getName());
        assertEquals(original.getBackupCount(), clone.getBackupCount());
        assertEquals(original.getAsyncBackupCount(), clone.getAsyncBackupCount());
        assertEquals(original.getCapacity(), clone.getCapacity());
    }

    // =================== set capacity ===========================

    @Test
    public void setCapacity() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setCapacity(1000);

        assertEquals(1000, config.getCapacity());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setCapacity_whenTooSmall() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setCapacity(0);
    }

    // =================== set backups count ===========================

    @Test
    public void setBackupCount() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setBackupCount(4);

        assertEquals(4, config.getBackupCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_whenTooSmall() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setBackupCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_whenTooLarge() {
        RingbufferConfig ringbufferConfig = new RingbufferConfig(NAME);

        ringbufferConfig.setBackupCount(MAX_BACKUP_COUNT + 1);
    }

    // =================== set async backup count ===========================

    @Test
    public void setAsyncBackupCount() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setAsyncBackupCount(4);

        assertEquals(4, config.getAsyncBackupCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenTooSmall() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setAsyncBackupCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenTooLarge() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setAsyncBackupCount(MAX_BACKUP_COUNT + 1);
    }

    // ============= get total backup count ====================

    @Test
    public void getTotalBackupCount() {
        RingbufferConfig config = new RingbufferConfig(NAME);
        config.setAsyncBackupCount(2);
        config.setBackupCount(3);

        int result = config.getTotalBackupCount();

        assertEquals(5, result);
    }

    // ================== retention =================================


    @Test(expected = IllegalArgumentException.class)
    public void setTimeToLiveSeconds_whenNegative() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setTimeToLiveSeconds(-1);
    }

    @Test
    public void setTimeToLiveSeconds() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        RingbufferConfig returned = config.setTimeToLiveSeconds(10);

        assertSame(returned, config);
        assertEquals(10, config.getTimeToLiveSeconds());
    }

    // ================== inMemoryFormat =================================

    @Test(expected = NullPointerException.class)
    public void setInMemoryFormat_whenNull() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setInMemoryFormat(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInMemoryFormat_whenNative() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        config.setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Test
    public void setInMemoryFormat() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        RingbufferConfig returned = config.setInMemoryFormat(InMemoryFormat.OBJECT);

        assertSame(config, returned);
        assertEquals(InMemoryFormat.OBJECT, config.getInMemoryFormat());
    }

    // ==================== RingbufferStoreConfig ================================

    @Test
    public void getRingbufferStoreConfig() {
        final RingbufferConfig config = new RingbufferConfig(NAME);
        final RingbufferStoreConfig ringbufferConfig = config.getRingbufferStoreConfig();
        assertNotNull(ringbufferConfig);
        assertFalse(ringbufferConfig.isEnabled());
    }

    @Test
    public void setRingbufferStoreConfig() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("myClassName");


        RingbufferConfig config = new RingbufferConfig(NAME);
        config.setRingbufferStoreConfig(ringbufferStoreConfig);

        assertEquals(ringbufferStoreConfig, config.getRingbufferStoreConfig());
    }

    // =================== getAsReadOnly ============================

    @Test
    public void getAsReadOnly() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig();

        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getName())
                .setBatchSize(2342);

        RingbufferConfig original = new RingbufferConfig(NAME)
                .setBackupCount(2)
                .setAsyncBackupCount(1)
                .setCapacity(10)
                .setTimeToLiveSeconds(400)
                .setRingbufferStoreConfig(ringbufferStoreConfig)
                .setMergePolicyConfig(mergePolicyConfig);

        RingbufferConfig readonly = new RingbufferConfigReadOnly(original);
        assertNotNull(readonly);

        assertEquals(original.getName(), readonly.getName());
        assertEquals(original.getBackupCount(), readonly.getBackupCount());
        assertEquals(original.getAsyncBackupCount(), readonly.getAsyncBackupCount());
        assertEquals(original.getCapacity(), readonly.getCapacity());
        assertEquals(original.getTimeToLiveSeconds(), readonly.getTimeToLiveSeconds());
        assertEquals(original.getInMemoryFormat(), readonly.getInMemoryFormat());
        assertEquals(original.getRingbufferStoreConfig(), readonly.getRingbufferStoreConfig());
        assertFalse("The read-only RingbufferStoreConfig should not be identity-equal to the original RingbufferStoreConfig",
                original.getRingbufferStoreConfig() == readonly.getRingbufferStoreConfig());
        assertEquals(original.getMergePolicyConfig(), readonly.getMergePolicyConfig());

        try {
            readonly.setCapacity(10);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.setAsyncBackupCount(1);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.setBackupCount(1);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.setTimeToLiveSeconds(1);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.setInMemoryFormat(InMemoryFormat.OBJECT);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.setRingbufferStoreConfig(null);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.getRingbufferStoreConfig().setEnabled(true);
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        try {
            readonly.setMergePolicyConfig(new MergePolicyConfig());
            fail();
        } catch (UnsupportedOperationException expected) {
        }

        original.setRingbufferStoreConfig(null);
        readonly = new RingbufferConfigReadOnly(original);

        assertNotNull(readonly.getRingbufferStoreConfig());
        assertFalse(readonly.getRingbufferStoreConfig().isEnabled());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(RingbufferConfig.class)
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .withPrefabValues(RingbufferStoreConfigReadOnly.class,
                              new RingbufferStoreConfigReadOnly(new RingbufferStoreConfig().setClassName("red")),
                              new RingbufferStoreConfigReadOnly(new RingbufferStoreConfig().setClassName("black")))
                      .withPrefabValues(MergePolicyConfig.class,
                              new MergePolicyConfig(PutIfAbsentMergePolicy.class.getName(), 100),
                              new MergePolicyConfig(DiscardMergePolicy.class.getName(), 200))
                      .verify();
    }
}
