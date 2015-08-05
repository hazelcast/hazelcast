package com.hazelcast.spi;

import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperationTest extends HazelcastTestSupport {

    @Test
    public void setResponseHandler_whenNull() {
        Operation op = new DummyOperation();
        op.setResponseHandler(null);
        assertNull(op.getOperationResponseHandler());
    }

    @Test
    public void setResponseHandler_whenNotNull() {
        Operation op = new DummyOperation();
        ResponseHandler responseHandler = mock(ResponseHandler.class);

        op.setResponseHandler(responseHandler);
        OperationResponseHandler found = op.getOperationResponseHandler();
        assertNotNull(found);

        // verify that the sendResponse is forwarded.
        found.sendResponse(op, "foo");
        verify(responseHandler).sendResponse("foo");

        // verify that the isLocal call is forwarded
        found.isLocal();
        verify(responseHandler).isLocal();
    }

    @Test
    public void getResponseHandler_whenResponseHandlerInstance() {
        Operation op = new DummyOperation();
        ResponseHandler responseHandler = mock(ResponseHandler.class);

        op.setResponseHandler(responseHandler);

        assertSame(responseHandler, op.getResponseHandler());
    }

    @Test
    public void getResponseHandler_whenNull() {
        Operation op = new DummyOperation();

        op.setOperationResponseHandler(null);

        assertNull(op.getResponseHandler());
    }

    @Test
    public void getResponseHandler_whenOperationResponseHandlerInstance() {
        Operation op = new DummyOperation();
        OperationResponseHandler operationResponseHandler = mock(OperationResponseHandler.class);

        op.setOperationResponseHandler(operationResponseHandler);

        ResponseHandler found = op.getResponseHandler();
        assertNotNull(found);

        // verify that the sendResponse is forwarded.
        found.sendResponse("foo");
        verify(operationResponseHandler).sendResponse(op, "foo");

        // verify that the isLocal call is forwarded
        found.isLocal();
        verify(operationResponseHandler).isLocal();
    }
}
