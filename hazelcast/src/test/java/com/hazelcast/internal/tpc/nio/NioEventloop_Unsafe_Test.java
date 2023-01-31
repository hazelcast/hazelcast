package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.Eventloop_Unsafe_Test;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.tpc.Eventloop;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class NioEventloop_Unsafe_Test extends Eventloop_Unsafe_Test {

    @Override
    public Eventloop create() {
        return new NioEventloop();
    }
}
