package com.hazelcast.internal.tpc;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class TpcEngine_ConfigurationTest {

    @Test(expected = NullPointerException.class)
    public void setEventLoopConfiguration_whenNull(){
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setEventloopConfiguration(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setEventLoopConfiguration_whenZero(){
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setEventloopCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setEventLoopConfiguration_whenNegative(){
        TpcEngine.Configuration configuration = new TpcEngine.Configuration();
        configuration.setEventloopCount(-1);
    }
}
