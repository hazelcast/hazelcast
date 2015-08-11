package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.RingbufferConfig.DEFAULT_ASYNC_BACKUP_COUNT;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_CAPACITY;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_SYNC_BACKUP_COUNT;
import static com.hazelcast.partition.InternalPartition.MAX_BACKUP_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RingbufferConfigTest {

    public static final String NAME = "someringbuffer";

    @Test
    public void testDefaultSetting() {
        RingbufferConfig config = new RingbufferConfig(NAME);
        assertEquals(NAME, config.getName());
        assertEquals(DEFAULT_SYNC_BACKUP_COUNT, config.getBackupCount());
        assertEquals(DEFAULT_ASYNC_BACKUP_COUNT, config.getAsyncBackupCount());
        assertEquals(DEFAULT_CAPACITY, config.getCapacity());
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

    // ================== inmemoryformat =================================


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

    // ==================== toString ================================

    @Test
    public void test_toString() {
        RingbufferConfig config = new RingbufferConfig(NAME);

        String s = config.toString();

        assertEquals("RingbufferConfig{name='someringbuffer', capacity=10000, backupCount=1, " +
                "asyncBackupCount=0, timeToLiveSeconds=0, inMemoryFormat=BINARY}", s);
    }

    // =================== getAsReadOnly ============================

    @Test
    public void getAsReadOnly() {
        RingbufferConfig original = new RingbufferConfig(NAME);
        original.setBackupCount(2).setAsyncBackupCount(1).setCapacity(10).setTimeToLiveSeconds(400);

        RingbufferConfig readonly = original.getAsReadOnly();
        assertNotNull(readonly);

        assertEquals(original.getName(), readonly.getName());
        assertEquals(original.getBackupCount(), readonly.getBackupCount());
        assertEquals(original.getAsyncBackupCount(), readonly.getAsyncBackupCount());
        assertEquals(original.getCapacity(), readonly.getCapacity());
        assertEquals(original.getTimeToLiveSeconds(), readonly.getTimeToLiveSeconds());
        assertEquals(original.getInMemoryFormat(), readonly.getInMemoryFormat());

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
    }
}
