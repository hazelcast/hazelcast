package com.hazelcast.nio.serialization;

import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class EnumTest {

    @Test
    public void testDataWriter() throws IOException {
        SerializationService ss = new SerializationServiceBuilder().build();

        TimeUnit timeUnit = TimeUnit.SECONDS;
        Data data = ss.toData(timeUnit);
        TimeUnit found = (TimeUnit)ss.toObject(data);
        assertSame(timeUnit,found);
      }
}
