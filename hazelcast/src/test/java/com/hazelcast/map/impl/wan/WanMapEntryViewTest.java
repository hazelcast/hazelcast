package com.hazelcast.map.impl.wan;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
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
public class WanMapEntryViewTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testDeSerialization() {
        String keyString = "keyData";
        String valueString = "valueData";
        Data keyData = serializationService.toData(keyString);
        Data valueData = serializationService.toData(valueString);
        WanMapEntryView<String, String> expected
                = new WanMapEntryView<String, String>(keyData, valueData, serializationService)
                .withCost(100)
                .withVersion(101)
                .withHits(102)
                .withLastAccessTime(103)
                .withLastUpdateTime(104)
                .withTtl(105)
                .withMaxIdle(106)
                .withCreationTime(107)
                .withExpirationTime(108)
                .withLastStoredTime(109);
        WanMapEntryView<String, String> actual
                = serializationService.toObject(serializationService.toData(expected));
        actual.setSerializationService(serializationService);

        assertEquals(expected, actual);
        assertEquals(keyString, actual.getKey());
        assertEquals(valueString, actual.getValue());
        assertEquals(keyString, expected.getKey());
        assertEquals(valueString, expected.getValue());

        assertEquals(100, actual.getCost());
        assertEquals(101, actual.getVersion());
        assertEquals(102, actual.getHits());
        assertEquals(103, actual.getLastAccessTime());
        assertEquals(104, actual.getLastUpdateTime());
        assertEquals(105, actual.getTtl());
        assertEquals(106, actual.getMaxIdle());
        assertEquals(107, actual.getCreationTime());
        assertEquals(108, actual.getExpirationTime());
        assertEquals(109, actual.getLastStoredTime());
    }
}