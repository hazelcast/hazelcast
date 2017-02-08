package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.cluster.impl.Versions;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.version.Version;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionedObjectDataInputAndOutputTest {

    private final InternalSerializationService iss = (InternalSerializationService)
            new DefaultSerializationServiceBuilder().setVersion(SerializationServiceV1.VERSION_1).build();

    @Test
    public void testVersionOnInput() {
        ObjectDataInputStream input = new ObjectDataInputStream(new ByteArrayInputStream(new byte[]{}), iss);
        Version version = Versions.V3_8;

        input.setVersion(version);
        assertEquals(version, input.getVersion());

    }

    @Test
    public void testVersionOnOutput() {
        ObjectDataOutputStream output = new ObjectDataOutputStream(new ByteArrayOutputStream(16), iss);
        Version version = Versions.V3_8;

        output.setVersion(version);
        assertEquals(version, output.getVersion());

    }

}