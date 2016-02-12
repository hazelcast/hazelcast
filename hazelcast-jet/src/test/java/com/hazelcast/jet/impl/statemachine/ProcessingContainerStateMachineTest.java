package com.hazelcast.jet.impl.statemachine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.container.ContainerRequest;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerResponse;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerState;
import com.hazelcast.jet.impl.application.DefaultExecutorContext;
import com.hazelcast.jet.impl.executor.StateMachineTaskExecutorImpl;
import com.hazelcast.jet.impl.statemachine.container.ProcessingContainerStateMachineImpl;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerExecuteRequest;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerFinalizedRequest;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerInterruptRequest;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerInterruptedRequest;
import com.hazelcast.jet.impl.statemachine.container.requests.ContainerStartRequest;
import com.hazelcast.jet.impl.statemachine.container.requests.InvalidateContainersRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
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
public class ProcessingContainerStateMachineTest extends HazelcastTestSupport {

    private StateMachineContext stateMachineContext;
    private ProcessingContainerStateMachineImpl stateMachine;
    private StateMachineTaskExecutorImpl executor;

    @Before
    public void setUp() throws Exception {
        stateMachineContext = new StateMachineContext().invoke();
        stateMachine = stateMachineContext.getStateMachine();
        executor = stateMachineContext.getExecutor();
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(ProcessingContainerState.NEW, stateMachine.currentState());
    }

    @Test(expected = ExecutionException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(new ContainerInterruptRequest());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingStartEvent_received() throws Exception {
        ProcessingContainerResponse response = processRequest(new ContainerStartRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizedEvent_received() throws Exception {
        ProcessingContainerResponse response = processRequest(new ContainerFinalizedRequest(mock(ProcessingContainer.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Execution_when_ProcessingExecuteEvent_received_and_stateWas_Awaiting() throws Exception {
        processRequest(new ContainerStartRequest());
        ProcessingContainerResponse response = processRequest(new ContainerExecuteRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.EXECUTION, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizeEvent_received_and_stateWas_Awaiting() throws Exception {
        processRequest(new ContainerStartRequest());
        ProcessingContainerResponse response = processRequest(new ContainerFinalizedRequest(mock(ProcessingContainer.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingInterruptedEvent_received_and_stateWas_Awaiting() throws Exception {
        processRequest(new ContainerStartRequest());
        ProcessingContainerResponse response = processRequest(new ContainerInterruptedRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.AWAITING, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Awaiting_when_ProcessingExecutionCompletedEvent_received_and_stateWas_Execution() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        ProcessingContainerResponse response = processRequest(new ContainerExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Interrupting_when_ProcessingInterruptEvent_received_and_stateWas_Execution() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        ProcessingContainerResponse response = processRequest(new ContainerInterruptRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.INTERRUPTING, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Awaiting_when_ProcessingInterruptedEvent_received_and_stateWas_Execution() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        ProcessingContainerResponse response = processRequest(new ContainerInterruptedRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingExecutionCompletedEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        processRequest(new ContainerInterruptRequest());
        ProcessingContainerResponse response = processRequest(new ContainerExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizeEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        processRequest(new ContainerInterruptRequest());
        ProcessingContainerResponse response = processRequest(new ContainerFinalizedRequest(mock(ProcessingContainer.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingInterruptedEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        processRequest(new ContainerInterruptRequest());
        ProcessingContainerResponse response = processRequest(new ContainerInterruptedRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Invalidated_when_ProcessingInvalidateContainerEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        processRequest(new ContainerInterruptRequest());
        ProcessingContainerResponse response = processRequest(new InvalidateContainersRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.INVALIDATED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Invalidated_when_ProcessingInvalidateContainerEvent_received_and_stateWas_Invalidated() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        processRequest(new ContainerInterruptRequest());
        processRequest(new InvalidateContainersRequest());
        ProcessingContainerResponse response = processRequest(new InvalidateContainersRequest());

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.INVALIDATED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizeEvent_received_and_stateWas_Invalidated() throws Exception {
        processRequest(new ContainerStartRequest());
        processRequest(new ContainerExecuteRequest());
        processRequest(new ContainerInterruptRequest());
        processRequest(new InvalidateContainersRequest());
        ProcessingContainerResponse response = processRequest(new ContainerFinalizedRequest(mock(ProcessingContainer.class)));

        assertTrue(response.isSuccess());
        assertEquals(ProcessingContainerState.FINALIZED, stateMachine.currentState());
    }

    private ProcessingContainerResponse processRequest(ContainerRequest request) throws InterruptedException, java.util.concurrent.ExecutionException {
        Future<ProcessingContainerResponse> future = stateMachine.handleRequest(request);
        executor.wakeUp();
        return future.get();
    }

    private class StateMachineContext {
        private StateMachineTaskExecutorImpl executor;
        private ProcessingContainerStateMachineImpl stateMachine;

        public StateMachineTaskExecutorImpl getExecutor() {
            return executor;
        }

        public ProcessingContainerStateMachineImpl getStateMachine() {
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
            when(executorContext.getDataContainerStateMachineExecutor()).thenReturn(executor);
            stateMachine = new ProcessingContainerStateMachineImpl(randomName(), requestProcessor, nodeEngine, context);
            return this;
        }
    }
}
