package com.hazelcast.internal.diagnostics;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class DistributionTest {

    private Distribution distribution;

    @Before
    public void setup(){
        distribution = new Distribution(32,1000);
    }

    @Test
    public void test(){
        distribution.record(TimeUnit.NANOSECONDS.toNanos(10));
    }
}
