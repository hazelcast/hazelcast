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
import com.hazelcast.jet.impl.statemachine.job.JobEvent;
import com.hazelcast.jet.impl.statemachine.job.JobResponse;
import com.hazelcast.jet.impl.statemachine.job.JobState;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachineRequest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.ManagedExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.statemachine.job.JobEvent.DEPLOYMENT_FAILURE;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.DEPLOYMENT_START;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.DEPLOYMENT_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.EXECUTION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.EXECUTION_START;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.EXECUTION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.FINALIZATION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.FINALIZATION_START;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.FINALIZATION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.INIT_FAILURE;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.INIT_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.INTERRUPTION_FAILURE;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.INTERRUPTION_START;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.INTERRUPTION_SUCCESS;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.SUBMIT_FAILURE;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.SUBMIT_START;
import static com.hazelcast.jet.impl.statemachine.job.JobEvent.SUBMIT_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class JobStateMachineTest extends HazelcastTestSupport {

    private StateMachineContext stateMachineContext;
    private JobStateMachine stateMachine;
    private StateMachineExecutor executor;

    @Before
    public void setUp() throws Exception {
        stateMachineContext = new StateMachineContext().invoke();
        stateMachine = stateMachineContext.getStateMachine();
        executor = stateMachineContext.getExecutor();
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test(expected = InvalidEventException.class)
    public void testInvalidTransition_throwsException() throws Exception {
        processRequest(getRequest(INTERRUPTION_FAILURE));
    }


    @Test
    public void testNextStateIs_InitSuccess_when_InitSuccess_received() throws Exception {
        JobResponse response = processRequest(getRequest(INIT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.INIT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InitFailure_when_InitFailure_received() throws Exception {
        JobResponse response = processRequest(getRequest(INIT_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.INIT_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_DeploymentInProgress_when_DeploymentStart_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        JobResponse response = processRequest(getRequest(DEPLOYMENT_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.DEPLOYMENT_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_DeploymentSuccess_when_DeploymentSuccess_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        JobResponse response = processRequest(getRequest(DEPLOYMENT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.DEPLOYMENT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_InitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_InitFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_InitFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_DeploymentSuccess_when_DeploymentSuccess_received_and_stateWas_DeploymentInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        JobResponse response = processRequest(getRequest(DEPLOYMENT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.DEPLOYMENT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_DeploymentFailure_when_DeploymentFailure_received_and_stateWas_DeploymentInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        JobResponse response = processRequest(getRequest(DEPLOYMENT_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.DEPLOYMENT_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitInProgress_when_SubmitStart_received_and_stateWas_DeploymentSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        JobResponse response = processRequest(getRequest(SUBMIT_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.SUBMIT_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_DeploymentSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitSuccess_when_SubmitSuccess_received_and_stateWas_DeploymentSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        JobResponse response = processRequest(getRequest(SUBMIT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.SUBMIT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_DeploymentFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_DeploymentFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitSuccess_when_SubmitSuccess_received_and_stateWas_SubmitInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        JobResponse response = processRequest(getRequest(SUBMIT_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.SUBMIT_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_SubmitFailure_when_SubmitFailure_received_and_stateWas_SubmitInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        JobResponse response = processRequest(getRequest(SUBMIT_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.SUBMIT_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInProgress_when_ExecutionStart_received_and_stateWas_SubmitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        JobResponse response = processRequest(getRequest(EXECUTION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionSuccess_received_and_stateWas_SubmitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        JobResponse response = processRequest(getRequest(EXECUTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_SubmitSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_SubmitFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_SubmitFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionSuccess_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        JobResponse response = processRequest(getRequest(EXECUTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionFailure_when_ExecutionFailure_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        JobResponse response = processRequest(getRequest(EXECUTION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionSuccess_when_InterruptionSuccess_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        JobResponse response = processRequest(getRequest(INTERRUPTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.INTERRUPTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionInProgress_when_InterruptionStart_received_and_stateWas_ExecutionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        JobResponse response = processRequest(getRequest(INTERRUPTION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.INTERRUPTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionInProgress_when_ExecutionFailure_received_and_stateWas_InterruptionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        JobResponse response = processRequest(getRequest(EXECUTION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionSuccess_when_InterruptionSuccess_received_and_stateWas_InterruptionInProgress() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        JobResponse response = processRequest(getRequest(INTERRUPTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.INTERRUPTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInProgress_when_ExecutionStart_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        JobResponse response = processRequest(getRequest(EXECUTION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_InterruptionSuccess_when_ExecutionFailure_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        JobResponse response = processRequest(getRequest(EXECUTION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.INTERRUPTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_InterruptionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(INTERRUPTION_START));
        processRequest(getRequest(INTERRUPTION_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionInProgress_when_ExecutionStart_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        JobResponse response = processRequest(getRequest(EXECUTION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_ExecutionSuccess_when_ExecutionSuccess_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        JobResponse response = processRequest(getRequest(EXECUTION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.EXECUTION_SUCCESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_ExecutionSuccess() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_SUCCESS));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }


    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_ExecutionFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_ExecutionFailure() throws Exception {
        processRequest(getRequest(INIT_SUCCESS));
        processRequest(getRequest(DEPLOYMENT_START));
        processRequest(getRequest(DEPLOYMENT_SUCCESS));
        processRequest(getRequest(SUBMIT_START));
        processRequest(getRequest(SUBMIT_SUCCESS));
        processRequest(getRequest(EXECUTION_START));
        processRequest(getRequest(EXECUTION_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationFailure_when_FinalizationFailure_received_and_stateWas_FinalizationInProgress() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        JobResponse response = processRequest(getRequest(FINALIZATION_FAILURE));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_FAILURE, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_FinalizationInProgress() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_FinalizationInProgress_when_FinalizationStart_received_and_stateWas_FinalizationFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        processRequest(getRequest(FINALIZATION_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_START));

        assertTrue(response.isSuccess());
        assertEquals(JobState.FINALIZATION_IN_PROGRESS, stateMachine.currentState());
    }

    @Test
    public void testNextStateIs_New_when_FinalizationSuccess_received_and_stateWas_FinalizationFailure() throws Exception {
        processRequest(getRequest(INIT_FAILURE));
        processRequest(getRequest(FINALIZATION_START));
        processRequest(getRequest(FINALIZATION_FAILURE));
        JobResponse response = processRequest(getRequest(FINALIZATION_SUCCESS));

        assertTrue(response.isSuccess());
        assertEquals(JobState.NEW, stateMachine.currentState());
    }


    private JobStateMachineRequest getRequest(JobEvent event) {
        return new JobStateMachineRequest(event);
    }

    private JobResponse processRequest(StateMachineRequest request) throws InterruptedException, ExecutionException {
        Future<JobResponse> future = stateMachine.handleRequest(request);
        executor.wakeUp();
        return future.get();
    }

    private class StateMachineContext {
        private StateMachineExecutor executor;
        private JobStateMachine stateMachine;


        public StateMachineExecutor getExecutor() {
            return executor;
        }

        public JobStateMachine getStateMachine() {
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
            when(nodeEngine.getLogger(any(Class.class))).thenReturn(mock(ILogger.class));
            when(nodeEngine.getExecutionService()).thenReturn(executionService);

            JobContext context = mock(JobContext.class);
            ExecutorContext executorContext = mock(ExecutorContext.class);
            when(context.getExecutorContext()).thenReturn(executorContext);
            when(context.getNodeEngine()).thenReturn(nodeEngine);
            executor = new StateMachineExecutor(randomName(), 1, 1, nodeEngine);
            when(executorContext.getJobStateMachineExecutor()).thenReturn(executor);
            stateMachine = new JobStateMachine(randomName(), requestProcessor, context);
            return this;
        }
    }
}
