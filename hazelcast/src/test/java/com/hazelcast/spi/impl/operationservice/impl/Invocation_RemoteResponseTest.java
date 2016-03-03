package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests that an invocation can receive a remote response.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_RemoteResponseTest extends HazelcastTestSupport {

    public static final boolean LOW_PRIORITY = false;
    private HazelcastInstance local;
    private HazelcastInstance remote;
    private OperationServiceImpl localOperationService;
    private InvocationRegistry localInvocationRegistry;
    private Connection c;
    private RemoteOperationResponseHandler responseHandler;

    @Before
    public void setup() {
        setLoggingLog4j();
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        local = cluster[0];
        remote = cluster[1];
        localOperationService = (OperationServiceImpl) getOperationService(local);
        localInvocationRegistry = localOperationService.invocationsRegistry;

        c = getConnectionManager(remote).getConnection(getAddress(local));
        responseHandler = new RemoteOperationResponseHandler(
                (OperationServiceImpl) getOperationService(remote), getSerializationService(remote));
    }

    private Invocation newRemoteInvocation(Operation op) {
        Invocation invocation = new TargetInvocation(
                getNodeEngineImpl(local), null, op, getAddress(remote), 0, 0, Long.MAX_VALUE, null, true);
        invocation.remote = true;
        return invocation;
    }

    @Test
    public void getResponse_andNoBackups() throws Exception {
        Operation op = new DummyOperation();
        Invocation invocation = new TargetInvocation(getNodeEngineImpl(local), null, op, getAddress(remote), 0, 0, Long.MAX_VALUE, null, true);
        invocation.remote = true;
        localInvocationRegistry.register(invocation);

        Object response = "someresponse";
        responseHandler.sendResponse(c, LOW_PRIORITY, op.getCallId(), 0, response);

        assertEquals(response, invocation.invocationFuture.get());
    }

    @Test
    public void getResponse_andSomeBackup() throws Exception {
        Operation op = new DummyOperation();
        final Invocation invocation = newRemoteInvocation(op);
        localInvocationRegistry.register(invocation);

        Object response = "someresponse";
        final int backupCount = 4;
        responseHandler.sendResponse(c, LOW_PRIORITY, op.getCallId(), backupCount, response);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(backupCount, invocation.backupsExpected);
            }
        });
    }

    @Test
    public void getBackupResponse() throws Exception {
        Operation op = new DummyOperation();
        final Invocation invocation = newRemoteInvocation(op);
        localInvocationRegistry.register(invocation);

        responseHandler.sendBackupResponse(getAddress(local), LOW_PRIORITY, op.getCallId());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, invocation.backupsCompleted);
            }
        });
    }

    @Test
    public void getErrorResponse() throws Exception {
        Operation op = new DummyOperation();
        Invocation invocation = newRemoteInvocation(op);
        localInvocationRegistry.register(invocation);

        Throwable response = new ExpectedRuntimeException();
        responseHandler.sendErrorResponse(c, LOW_PRIORITY, op.getCallId(), response);

        try {
            invocation.invocationFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(ExpectedRuntimeException.class, e.getCause());
        }
    }

    @Test
    public void getTimeoutResponse() throws Exception {
        Operation op = new DummyOperation();
        final Invocation invocation = newRemoteInvocation(op);
        localInvocationRegistry.register(invocation);

        responseHandler.sendTimeoutResponse(c, LOW_PRIORITY, op.getCallId());

        try {
            invocation.invocationFuture.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(ExpectedRuntimeException.class, e.getCause());
        }
    }
}
