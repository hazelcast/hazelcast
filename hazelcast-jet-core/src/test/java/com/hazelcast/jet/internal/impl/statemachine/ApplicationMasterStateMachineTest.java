package com.hazelcast.jet.internal.impl.statemachine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.internal.api.statemachine.container.ContainerRequest;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.internal.impl.application.DefaultExecutorContext;
import com.hazelcast.jet.internal.impl.executor.StateMachineTaskExecutorImpl;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.ApplicationMasterStateMachineImpl;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecuteApplicationRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionErrorRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionPlanBuilderRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecutionPlanReadyRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.FinalizeApplicationRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.InterruptApplicationRequest;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ApplicationMasterStateMachineTest extends HazelcastTestSupport {

    private StateMachineContext stateMachineContext;
    private ApplicationMasterStateMachineImpl stateMachine;
    private StateMachineTaskExecutorImpl executor;

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

    @Test(expected = ExecutionException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(new InterruptApplicationRequest());
    }


    @Test
    public void testNextStateIs_DagSubmitted_when_SubmitDag_received() throws Exception {
        ApplicationMasterResponse response = processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.DAG_SUBMITTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received() throws Exception {
        ApplicationMasterResponse response = processRequest(new FinalizeApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_DagSubmitted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        ApplicationMasterResponse response = processRequest(new FinalizeApplicationRequest());

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
        ApplicationMasterResponse response = processRequest(new ExecuteApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionError_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionCompleted_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupting_when_InterruptExecution_received_and_stateWas_Executing() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        ApplicationMasterResponse response = processRequest(new InterruptApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_INTERRUPTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new ExecutionCompletedRequest());
        ApplicationMasterResponse response = processRequest(new ExecuteApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new ExecutionCompletedRequest());
        ApplicationMasterResponse response = processRequest(new FinalizeApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new ExecuteApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionError_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new FinalizeApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailed_when_ExecutionInterrupted_received_and_stateWas_ExecutionFailed() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new ExecutionErrorRequest(mock(Throwable.class)));
        ApplicationMasterResponse response = processRequest(new ExecutionInterruptedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_FAILED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupted_when_ExecutionInterrupted_received_and_stateWas_ExecutionInterrupting() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new InterruptApplicationRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionInterruptedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_INTERRUPTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInterrupted_when_ExecutionError_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new InterruptApplicationRequest());
        processRequest(new ExecutionInterruptedRequest());
        ApplicationMasterResponse response = processRequest(new ExecutionErrorRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTION_INTERRUPTED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Executing_when_Execute_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new InterruptApplicationRequest());
        processRequest(new ExecutionInterruptedRequest());
        ApplicationMasterResponse response = processRequest(new ExecuteApplicationRequest());

        assertTrue(response.isSuccess());
        assertEquals(ApplicationMasterState.EXECUTING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_Finalize_received_and_stateWas_ExecutionInterrupted() throws Exception {
        processRequest(new ExecutionPlanBuilderRequest(mock(DAG.class)));
        processRequest(new ExecutionPlanReadyRequest());
        processRequest(new ExecuteApplicationRequest());
        processRequest(new InterruptApplicationRequest());
        processRequest(new ExecutionInterruptedRequest());
        ApplicationMasterResponse response = processRequest(new FinalizeApplicationRequest());

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
        private StateMachineTaskExecutorImpl executor;
        private ApplicationMasterStateMachineImpl stateMachine;

        public StateMachineTaskExecutorImpl getExecutor() {
            return executor;
        }

        public ApplicationMasterStateMachineImpl getStateMachine() {
            return stateMachine;
        }

        public StateMachineContext invoke() {
            StateMachineRequestProcessor requestProcessor = mock(StateMachineRequestProcessor.class);
            HazelcastInstance instance = mock(HazelcastInstance.class);
            when(instance.getName()).thenReturn(randomName());
            NodeEngine nodeEngine = mock(NodeEngine.class);
            when(nodeEngine.getHazelcastInstance()).thenReturn(instance);
            when(nodeEngine.getLogger(anyString())).thenReturn(mock(ILogger.class));
            ApplicationContext context = mock(ApplicationContext.class);
            DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);
            when(context.getExecutorContext()).thenReturn(executorContext);
            executor = new StateMachineTaskExecutorImpl(randomName(), 1, 1, nodeEngine);
            when(executorContext.getApplicationMasterStateMachineExecutor()).thenReturn(executor);
            stateMachine = new ApplicationMasterStateMachineImpl(randomName(), requestProcessor, nodeEngine, context);
            return this;
        }
    }
}
