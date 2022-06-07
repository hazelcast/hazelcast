package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.AsyncFileTest;
import com.hazelcast.tpc.engine.Eventloop;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class IOUringAsyncFileTest extends AsyncFileTest {

    @Override
    public Eventloop newEventloop() {
        return new IOUringEventloop();
    }
}
