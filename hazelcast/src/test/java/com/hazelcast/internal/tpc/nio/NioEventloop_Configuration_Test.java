package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Eventloop_Configuration_Test;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class NioEventloop_Configuration_Test extends Eventloop_Configuration_Test {

    @Override
    public Eventloop.Configuration create() {
        return new NioEventloop.NioConfiguration();
    }
}
