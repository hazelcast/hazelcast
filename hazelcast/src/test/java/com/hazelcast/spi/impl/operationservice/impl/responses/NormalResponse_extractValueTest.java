package com.hazelcast.spi.impl.operationservice.impl.responses;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse.extractValue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NormalResponse_extractValueTest {

    private SerializationServiceV1 serializationService;

    @Before
    public void setup() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        serializationService = defaultSerializationServiceBuilder
                .setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void whenNonDataAndDeserialize() {
        Object value = "foo";
        NormalResponse normalResponse = new NormalResponse(value, 0, 0, false);
        byte[] bytes = serializationService.toBytes(normalResponse);

        Object actual = extractValue(bytes, serializationService, true);
        assertEquals(value, actual);
    }

    @Test
    public void whenDataAndNotDeserialize() {
        HeapData value = serializationService.toData("foo");
        NormalResponse normalResponse = new NormalResponse(value, 0, 0, false);
        byte[] bytes = serializationService.toBytes(normalResponse);

        Object actual = extractValue(bytes, serializationService, false);

        assertEquals(value, actual);
    }

    @Test
    public void whenDataAndDeserialize() {
        HeapData value = serializationService.toData("foo");
        NormalResponse normalResponse = new NormalResponse(value, 0, 0, false);
        byte[] bytes = serializationService.toBytes(normalResponse);

        Object actual = extractValue(bytes, serializationService, true);

        assertEquals("foo", actual);
    }
}
