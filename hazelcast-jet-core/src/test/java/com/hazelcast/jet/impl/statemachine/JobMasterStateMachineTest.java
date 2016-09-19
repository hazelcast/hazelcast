package com.hazelcast.jet.impl.statemachine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerResponse;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerState;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.job.ExecutorContext;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.statemachine.jobmanager.JobManagerStateMachine;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecuteJobRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionErrorRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionPlanBuilderRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecutionPlanReadyRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.FinalizeJobRequest;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.InterruptJobRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class JobMasterStateMachineTest extends HazelcastTestSupport {

    private StateMachineContext stateMachineContext;
    private JobManagerStateMachine stateMachine;
    private StateMachineExecutor executor;

    @Before
    public void setUp() throws Exception {
        stateMachineContext = new StateMachineContext().invoke();
        stateMachine = stateMachineContext.getStateMachine();
        executor = stateMachineContext.getExecutor();
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(JobManagerState.NEW, stateMachine.currentState());
    }

    @Test(expected = InvalidEventException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(new InterruptJobRequest());
    }


    @Test
    public void testNextStateIs_DagSubmitted_when_SubmitDag_received() throws Exception {
        JobManagerResponse response = processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.DAG_SUBMITTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received() throws Exception {
        JobManagerResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.FINALIZED, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        JobManagerResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ReadyForExecution_when_ExecutionPlanReady_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        JobManagerResponse response = processRequest(new ExecutionPlanReadyRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.READY_FOR_EXECUTION, stateMachine.currentState());
    }

    @Ignore
    @Test
    public void testNextStateIs_InvalidDag_when_ExecutionPlanBuildFailed_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        // no matching request is found for EXECUTION_PLAN_BUILD_FAILED event.
        assertEquals(JobManagerState.INVALID_DAG, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ReadyForExecution() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        JobManagerResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionError_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        JobManagerResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionCompleted_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        JobManagerResponse response = processRequest(new ExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupting_when_InterruptExecution_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        JobManagerResponse response = processRequest(new InterruptJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_INTERRUPTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionCompletedRequest());
        JobManagerResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionCompletedRequest());
        JobManagerResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        JobManagerResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionError_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        JobManagerResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        JobManagerResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionInterrupted_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        JobManagerResponse response = processRequest(new ExecutionInterruptedRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupted_when_ExecutionInterrupted_received_and_stateWas_ExecutionInterrupting() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        JobManagerResponse response = processRequest(new ExecutionInterruptedRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_INTERRUPTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupted_when_ExecutionError_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        processRequest(new ExecutionInterruptedRequest());
        JobManagerResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTION_INTERRUPTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        processRequest(new ExecutionInterruptedRequest());
        JobManagerResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        processRequest(new ExecutionInterruptedRequest());
        JobManagerResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(JobManagerState.FINALIZED, stateMachine.currentState());
    }

    @Ignore
    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_InvalidDag() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        // no matching request is found for EXECUTION_PLAN_BUILD_FAILED event.
        assertEquals(JobManagerState.INVALID_DAG, stateMachine.currentState());
    }


    private JobManagerResponse processRequest(StateMachineRequest request) throws InterruptedException, java.util.concurrent.ExecutionException {
        Future<JobManagerResponse> future = stateMachine.handleRequest(request);
        executor.wakeUp();
        return future.get();
    }

    private class StateMachineContext {
        private StateMachineExecutor executor;
        private JobManagerStateMachine stateMachine;

        public StateMachineExecutor getExecutor() {
            return executor;
        }

        public JobManagerStateMachine getStateMachine() {
            return stateMachine;
        }

        public StateMachineContext invoke() {
            StateMachineEventHandler requestProcessor = mock(StateMachineEventHandler.class);

            HazelcastInstance instance = mock(HazelcastInstance.class);
            when(instance.getName()).thenReturn(randomName());

            ExecutionService executionService = mock(ExecutionService.class);
            when(executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR))
                    .thenReturn(mock(ManagedExecutorService.class));

            NodeEngine nodeEngine = mock(NodeEngine.class);
            when(nodeEngine.getHazelcastInstance()).thenReturn(instance);
            when(nodeEngine.getLogger(anyString())).thenReturn(mock(ILogger.class));
            when(nodeEngine.getLogger(any(Class.class))).thenReturn(mock(ILogger.class));
            when(nodeEngine.getExecutionService()).thenReturn(executionService);

            JobContext context = mock(JobContext.class);
            ExecutorContext executorContext = mock(ExecutorContext.class);
            when(context.getExecutorContext()).thenReturn(executorContext);
            when(context.getNodeEngine()).thenReturn(nodeEngine);

            executor = new StateMachineExecutor(randomName(), 1, 1, nodeEngine);
            when(executorContext.getJobManagerStateMachineExecutor()).thenReturn(executor);
            stateMachine = new JobManagerStateMachine(randomName(), requestProcessor, context);
            return this;
        }
    }
}
