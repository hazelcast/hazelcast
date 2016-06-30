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
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.application.DefaultExecutorContext;
import com.hazelcast.jet.impl.executor.StateMachineTaskExecutorImpl;
import com.hazelcast.jet.impl.statemachine.InvalidEventException;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.statemachine.application.ApplicationResponse;
import com.hazelcast.jet.impl.statemachine.application.ApplicationState;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachineImpl;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachineRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.EXECUTION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.EXECUTION_START;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.EXECUTION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.FINALIZATION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.FINALIZATION_START;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.FINALIZATION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.INIT_FAILURE;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.INIT_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.INTERRUPTION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.INTERRUPTION_START;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.INTERRUPTION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.LOCALIZATION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.LOCALIZATION_START;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.LOCALIZATION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.SUBMIT_FAILURE;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.SUBMIT_START;
import static com.hazelcast.jet.impl.statemachine.application.ApplicationEvent.SUBMIT_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ApplicationStateMachineTest extends HazelcastTestSupport {

    private StateMachineContext stateMachineContext;
    private ApplicationStateMachineImpl stateMachine;
    private StateMachineTaskExecutorImpl executor;

    @Before
    public void setUp() throws Exception {
        stateMachineContext = new StateMachineContext().invoke();
        stateMachine = stateMachineContext.getStateMachine();
        executor = stateMachineContext.getExecutor();
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test(expected = InvalidEventException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(getRequest(INTERRUPTION_FAILURE));
    }


    @Test
    public void testNextStateIs_InitSuccess_when_InitSuccess_received() throws Exception {
        ApplicationResponse response = processRequest(getRequest(INIT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.INIT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InitFailure_when_InitFailure_received() throws Exception {
        ApplicationResponse response = processRequest(getRequest(INIT_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.INIT_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_LocalizationInProgress_when_LocalizationStart_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(LOCALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.LOCALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_LocalizationSuccess_when_LocalizationSuccess_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(LOCALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.LOCALIZATION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_InitFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_InitFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_LocalizationSuccess_when_LocalizationSuccess_received_and_stateWas_LocalizationInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        ApplicationResponse response = processRequest(getRequest(LOCALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.LOCALIZATION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_LocalizationFailure_when_LocalizationFailure_received_and_stateWas_LocalizationInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        ApplicationResponse response = processRequest(getRequest(LOCALIZATION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.LOCALIZATION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitInProgress_when_SubmitStart_received_and_stateWas_LocalizationSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(SUBMIT_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.SUBMIT_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_LocalizationSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitSuccess_when_SubmitSuccess_received_and_stateWas_LocalizationSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(SUBMIT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.SUBMIT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_LocalizationFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_LocalizationFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitSuccess_when_SubmitSuccess_received_and_stateWas_SubmitInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        ApplicationResponse response = processRequest(getRequest(SUBMIT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.SUBMIT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitFailure_when_SubmitFailure_received_and_stateWas_SubmitInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        ApplicationResponse response = processRequest(getRequest(SUBMIT_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.SUBMIT_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInProgress_when_ExecutionStart_received_and_stateWas_SubmitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionSuccess_received_and_stateWas_SubmitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_SubmitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_SubmitFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_SubmitFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionSuccess_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailure_when_ExecutionFailure_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionSuccess_when_InterruptionSuccess_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        ApplicationResponse response = processRequest(getRequest(INTERRUPTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.INTERRUPTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionInProgress_when_InterruptionStart_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        ApplicationResponse response = processRequest(getRequest(INTERRUPTION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.INTERRUPTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionInProgress_when_ExecutionFailure_received_and_stateWas_InterruptionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionSuccess_when_InterruptionSuccess_received_and_stateWas_InterruptionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        ApplicationResponse response = processRequest(getRequest(INTERRUPTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.INTERRUPTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInProgress_when_ExecutionStart_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionSuccess_when_ExecutionFailure_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.INTERRUPTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInProgress_when_ExecutionStart_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionSuccess_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(EXECUTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_ExecutionFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_ExecutionFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(LOCALIZATION_START));
        processRequest(getRequest(LOCALIZATION_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationFailure_when_FinalizationFailure_received_and_stateWas_FinalizationInProgress() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_FinalizationInProgress() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_FinalizationFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        processRequest(getRequest(FINALIZATION_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_FinalizationFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        processRequest(getRequest(FINALIZATION_FAILURE));
        ApplicationResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(ApplicationState.NEW, stateMachine.currentState());
    }


    private ApplicationStateMachineRequest getRequest(ApplicationEvent event) {
        return new ApplicationStateMachineRequest(event);
    }

    private ApplicationResponse processRequest(StateMachineRequest request) throws InterruptedException, ExecutionException {
        Future<ApplicationResponse> future = stateMachine.handleRequest(request);
        executor.wakeUp();
        return future.get();
    }

    private class StateMachineContext {
        private StateMachineTaskExecutorImpl executor;
        private ApplicationStateMachineImpl stateMachine;


        public StateMachineTaskExecutorImpl getExecutor() {
            return executor;
        }

        public ApplicationStateMachineImpl getStateMachine() {
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
            when(nodeEngine.getLogger(any(Class.class))).thenReturn(mock(ILogger.class));
            when(nodeEngine.getExecutionService()).thenReturn(executionService);

            ApplicationContext context = mock(ApplicationContext.class);
            DefaultExecutorContext executorContext = mock(DefaultExecutorContext.class);
            when(context.getExecutorContext()).thenReturn(executorContext);
            when(context.getNodeEngine()).thenReturn(nodeEngine);
            executor = new StateMachineTaskExecutorImpl(randomName(), 1, 1, nodeEngine);
            when(executorContext.getApplicationStateMachineExecutor()).thenReturn(executor);
            stateMachine = new ApplicationStateMachineImpl(randomName(), requestProcessor, nodeEngine, context);
            return this;
        }
    }
}
