package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SampleableConcurrentHashMapTest extends HazelcastTestSupport {

    @Test
    public void samplesSuccessfullyRetrieved() {
        final int ENTRY_COUNT = 100;
        final int SAMPLE_COUNT = 15;
        SampleableConcurrentHashMap<Integer, Integer> sampleableConcurrentHashMap =
                new SampleableConcurrentHashMap<Integer, Integer>(ENTRY_COUNT);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            sampleableConcurrentHashMap.put(i, i);
        }

        Iterable<SampleableConcurrentHashMap<Integer, Integer>.SamplingEntry> samples =
                sampleableConcurrentHashMap.getRandomSamples(SAMPLE_COUNT);
        assertNotNull(samples);

        int sampleCount = 0;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (SampleableConcurrentHashMap<Integer, Integer>.SamplingEntry sample : samples) {
            map.put(sample.getKey(), sample.getValue());
            sampleCount++;
        }
        // Sure that there is enough sample as we expected
        assertEquals(SAMPLE_COUNT, sampleCount);
        // Sure that all samples are different
        assertEquals(SAMPLE_COUNT, map.size());
    }

}
