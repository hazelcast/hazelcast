/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.statemachine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.job.ExecutorContext;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerResponse;
import com.hazelcast.jet.impl.runtime.VertexRunnerState;
import com.hazelcast.jet.impl.statemachine.runner.VertexRunnerStateMachine;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerExecuteRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerFinalizedRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerInterruptRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerInterruptedRequest;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerStartRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class VertexRunnerStateMachineTest extends HazelcastTestSupport {

    private StateMachineContext stateMachineContext;
    private VertexRunnerStateMachine stateMachine;
    private StateMachineExecutor executor;

    @Before
    public void setUp() throws Exception {
        stateMachineContext = new StateMachineContext().invoke();
        stateMachine = stateMachineContext.getStateMachine();
        executor = stateMachineContext.getExecutor();
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(VertexRunnerState.NEW, stateMachine.currentState());
    }

    @Test(expected = InvalidEventException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(new VertexRunnerInterruptRequest());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingStartEvent_received() throws Exception {
        VertexRunnerResponse response = processRequest(new VertexRunnerStartRequest());

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizedEvent_received() throws Exception {
        VertexRunnerResponse response = processRequest(new VertexRunnerFinalizedRequest(mock(VertexRunner.class)));

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Execution_when_ProcessingExecuteEvent_received_and_stateWas_Awaiting() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerExecuteRequest());

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.EXECUTION, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizeEvent_received_and_stateWas_Awaiting() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerFinalizedRequest(mock(VertexRunner.class)));

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingInterruptedEvent_received_and_stateWas_Awaiting() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerInterruptedRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.AWAITING, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Awaiting_when_ProcessingExecutionCompletedEvent_received_and_stateWas_Execution() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        processRequest(new VertexRunnerExecuteRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Interrupting_when_ProcessingInterruptEvent_received_and_stateWas_Execution() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        processRequest(new VertexRunnerExecuteRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerInterruptRequest());

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.INTERRUPTING, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_Awaiting_when_ProcessingInterruptedEvent_received_and_stateWas_Execution() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        processRequest(new VertexRunnerExecuteRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerInterruptedRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingExecutionCompletedEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        processRequest(new VertexRunnerExecuteRequest());
        processRequest(new VertexRunnerInterruptRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerExecutionCompletedRequest());

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.AWAITING, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Finalized_when_ProcessingFinalizeEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        processRequest(new VertexRunnerExecuteRequest());
        processRequest(new VertexRunnerInterruptRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerFinalizedRequest(mock(VertexRunner.class)));

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.FINALIZED, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_Awaiting_when_ProcessingInterruptedEvent_received_and_stateWas_Interrupting() throws Exception {
        processRequest(new VertexRunnerStartRequest());
        processRequest(new VertexRunnerExecuteRequest());
        processRequest(new VertexRunnerInterruptRequest());
        VertexRunnerResponse response = processRequest(new VertexRunnerInterruptedRequest(mock(Throwable.class)));

        assertTrue(response.isSuccess());
        assertEquals(VertexRunnerState.AWAITING, stateMachine.currentState());
    }


    private VertexRunnerResponse processRequest(StateMachineRequest request) throws InterruptedException, java.util.concurrent.ExecutionException {
        Future<VertexRunnerResponse> future = stateMachine.handleRequest(request);
        executor.wakeUp();
        return future.get();
    }

    private class StateMachineContext {
        private StateMachineExecutor executor;
        private VertexRunnerStateMachine stateMachine;

        public StateMachineExecutor getExecutor() {
            return executor;
        }

        public VertexRunnerStateMachine getStateMachine() {
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
            when(nodeEngine.getLogger(Matchers.any(Class.class))).thenReturn(mock(ILogger.class));
            when(nodeEngine.getExecutionService()).thenReturn(executionService);

            JobContext context = mock(JobContext.class);
            ExecutorContext executorContext = mock(ExecutorContext.class);
            when(context.getExecutorContext()).thenReturn(executorContext);
            when(context.getNodeEngine()).thenReturn(nodeEngine);
            executor = new StateMachineExecutor(randomName(), 1, 1, nodeEngine);
            when(executorContext.getVertexManagerStateMachineExecutor()).thenReturn(executor);
            stateMachine = new VertexRunnerStateMachine(randomName(), requestProcessor, context);
            return this;
        }
    }
}
