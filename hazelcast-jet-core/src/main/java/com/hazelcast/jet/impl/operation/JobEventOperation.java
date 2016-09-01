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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.statemachine.job.JobEvent;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachineRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class JobEventOperation extends JetOperation {
    private JobEvent jobEvent;

    @SuppressWarnings("unused")
    public JobEventOperation() {
    }

    public JobEventOperation(String name, JobEvent jobEvent) {
        super(name);
        this.jobEvent = jobEvent;
    }

    @Override
    public void run() throws Exception {
        JobContext context = getJobContext();

        synchronized (context) {
            JobStateMachine jobStateMachine = context.getJobStateMachine();

            JobConfig config = context.getJobConfig();
            long secondsToAwait = config.getSecondsToAwait();

            Future future = jobStateMachine.handleRequest(new JobStateMachineRequest(this.jobEvent));

            context.getExecutorContext().getJobStateMachineExecutor().wakeUp();
            future.get(secondsToAwait, TimeUnit.SECONDS);
        }
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(this.jobEvent);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.jobEvent = in.readObject();
    }
}
