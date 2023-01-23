package com.hazelcast.jet.mongodb;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class MongoSerializationTest {

    static InternalSerializationService serializationService;

    @BeforeClass
    public static void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testTransactionOptions() {
        TransactionOptions expected = TransactionOptions.builder()
                .maxCommitTime(1L, TimeUnit.HOURS)
                .readPreference(ReadPreference.primaryPreferred())
                .writeConcern(WriteConcern.MAJORITY)
                .readConcern(ReadConcern.AVAILABLE)
                .build();
        Data data = serializationService.toData(expected);
        TransactionOptions actual = serializationService.toObject(data);
        assertEquals(expected, actual);
    }

}

