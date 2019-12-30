package com.hazelcast.cache.impl.wan;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanCacheEntryViewTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testDeSerialization() {
        String keyString = "keyData";
        String valueString = "valueData";
        Data keyData = serializationService.toData(keyString);
        Data valueData = serializationService.toData(valueString);
        WanCacheEntryView<String, String> expected
                = new WanCacheEntryView<>(keyData, valueData, 100, 101, 102, 103);
        expected.setSerializationService(serializationService);

        WanCacheEntryView<String, String> actual
                = serializationService.toObject(serializationService.toData(expected));
        actual.setSerializationService(serializationService);

        assertEquals(expected, actual);
        assertEquals(keyString, actual.getKey());
        assertEquals(valueString, actual.getValue());
        assertEquals(keyString, expected.getKey());
        assertEquals(valueString, expected.getValue());

        assertEquals(100, actual.getCreationTime());
        assertEquals(101, actual.getExpirationTime());
        assertEquals(102, actual.getLastAccessTime());
        assertEquals(103, actual.getHits());
    }
}