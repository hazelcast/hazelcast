package com.hazelcast.spi;

import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationTest {

    // test for https://github.com/hazelcast/hazelcast/issues/11375
    @Test
    public void sendResponse_whenResponseHandlerIsNull_andThrowableValue_thenNoNPE(){
        Operation op = new DummyOperation();
        op.sendResponse(new Exception());
    }

    // test for https://github.com/hazelcast/hazelcast/issues/11375
    @Test
    public void sendResponse_whenResponseHandlerIsNull_andNoThrowableValue_thenNoNPE(){
        Operation op = new DummyOperation();
        op.sendResponse("foo");
    }
}
