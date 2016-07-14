package com.hazelcast.jet.impl.statemachine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.job.ExecutorContext;
import com.hazelcast.jet.impl.container.ContainerRequest;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.statemachine.applicationmaster.ApplicationMasterStateMachine;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecuteJobRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionErrorRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionPlanBuilderRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionPlanReadyRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.FinalizeJobRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.InterruptJobRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

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
    private ApplicationMasterStateMachine stateMachine;
    private StateMachineExecutor executor;

    @Before
    public void setUp() throws Exception {
        stateMachineContext = new StateMachineContext().invoke();
        stateMachine = stateMachineContext.getStateMachine();
        executor = stateMachineContext.getExecutor();
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(ApplicationMasterState.NEW, stateMachine.currentState());
    }

    @Test(expected = InvalidEventException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(new InterruptJobRequest());
    }


    @Test
    public void testNextStateIs_DagSubmitted_when_SubmitDag_received() throws Exception {
        ApplicationMasterResponse response = processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.DAG_SUBMITTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received() throws Exception {
        ApplicationMasterResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        ApplicationMasterResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ReadyForExecution_when_ExecutionPlanReady_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        ApplicationMasterResponse response = processRequest(new ExecutionPlanReadyRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.READY_FOR_EXECUTION, stateMachine.currentState());
    }

    @Ignore
    @Test
    public void testNextStateIs_InvalidDag_when_ExecutionPlanBuildFailed_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        // no matching request is found for EXECUTION_PLAN_BUILD_FAILED event.
        assertEquals(ApplicationMasterState.INVALID_DAG, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ReadyForExecution() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        ApplicationMasterResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionError_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionCompleted_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupting_when_InterruptExecution_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        ApplicationMasterResponse response = processRequest(new InterruptJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_INTERRUPTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionCompletedRequest());
        ApplicationMasterResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionCompletedRequest());
        ApplicationMasterResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionError_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionInterrupted_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new ExecutionInterruptedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupted_when_ExecutionInterrupted_received_and_stateWas_ExecutionInterrupting() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionInterruptedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_INTERRUPTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupted_when_ExecutionError_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        processRequest(new ExecutionInterruptedRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_INTERRUPTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        processRequest(new ExecutionInterruptedRequest());
        ApplicationMasterResponse response = processRequest(new ExecuteJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteJobRequest());
        processRequest(new InterruptJobRequest());
        processRequest(new ExecutionInterruptedRequest());
        ApplicationMasterResponse response = processRequest(new FinalizeJobRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }

    @Ignore
    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_InvalidDag() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        // no matching request is found for EXECUTION_PLAN_BUILD_FAILED event.
        assertEquals(ApplicationMasterState.INVALID_DAG, stateMachine.currentState());
    }


    private ApplicationMasterResponse processRequest(ContainerRequest request) throws InterruptedException, java.util.concurrent.ExecutionException {
        Future<ApplicationMasterResponse> future = stateMachine.handleRequest(request);
        executor.wakeUp();
        return future.get();
    }

    private class StateMachineContext {
        private StateMachineExecutor executor;
        private ApplicationMasterStateMachine stateMachine;

        public StateMachineExecutor getExecutor() {
            return executor;
        }

        public ApplicationMasterStateMachine getStateMachine() {
            return stateMachine;
        }

        public StateMachineContext invoke() {
            StateMachineRequestProcessor requestProcessor = mock(StateMachineRequestProcessor.class);

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
            when(executorContext.getApplicationMasterStateMachineExecutor()).thenReturn(executor);
            stateMachine = new ApplicationMasterStateMachine(randomName(), requestProcessor, nodeEngine, context);
            return this;
        }
    }
}
