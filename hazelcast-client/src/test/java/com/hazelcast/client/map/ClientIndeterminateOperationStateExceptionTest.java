package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.properties.GroupProperty.FAIL_ON_INDETERMINATE_OPERATION_STATE;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientIndeterminateOperationStateExceptionTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance instance1;

    private HazelcastInstance instance2;

    private HazelcastInstance client;

    @Before
    public void init() {
        Config config = new Config();
        config.setProperty(OPERATION_BACKUP_TIMEOUT_MILLIS.getName(), String.valueOf(1000));
        config.setProperty(FAIL_ON_INDETERMINATE_OPERATION_STATE.getName(), String.valueOf(true));

        instance1 = factory.newHazelcastInstance(config);
        instance2 = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();

        warmUpPartitions(instance1, instance2);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void shouldFail_whenBackupAckMissed() throws InterruptedException, TimeoutException {
        dropOperationsBetween(instance1, instance2, SpiDataSerializerHook.F_ID, singletonList(SpiDataSerializerHook.BACKUP));

        String key = generateKeyOwnedBy(instance1);

        IMap<Object, Object> map = client.getMap(randomMapName());
        try {
            map.put(key, key);
            fail();
        } catch (IndeterminateOperationStateException expected) {
        }

        assertEquals(key, map.get(key));
    }

}
