package com.hazelcast.internal.diagnostics;

import org.junit.Before;
import org.junit.Test;

public class DistributionTest {

    private Distribution distribution;

    @Before
    public void setup(){
        distribution = new Distribution(32,1000);
    }

    @Test
    public void test(){
        distribution.record(20000000000l);
    }
}
