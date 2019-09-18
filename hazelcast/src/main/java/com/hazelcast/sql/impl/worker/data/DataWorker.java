/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker.data;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.InboxKey;
import com.hazelcast.sql.impl.worker.AbstractWorker;
import com.hazelcast.sql.impl.worker.control.StripeDeployment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Worker which process data requests.
 */
public class DataWorker extends AbstractWorker<DataTask> {
    /** Stripe not mapped to any thread yet. */
    public static final int UNMAPPED_STRIPE = -1;

    /** Registered inboxes. */
    private final Map<InboxKey, AbstractInbox> inboxes = new HashMap<>();

    /** Thread index. */
    private final int thread;

    public DataWorker(NodeEngine nodeEngine, int thread) {
        super(nodeEngine);

        this.thread = thread;
    }

    @Override
    protected void executeTask(DataTask task) {
        if (task instanceof StartStripeDataTask)
            handleStartStripeTask((StartStripeDataTask)task);
        else if (task instanceof BatchDataTask)
            handleBatchTask((BatchDataTask)task);
        else if (task instanceof RootDataTask)
            handleRootTask((RootDataTask)task);
    }

    /**
     * Start stripe execution.
     *
     * @param task Task descriptor.
     */
    private void handleStartStripeTask(StartStripeDataTask task) {
        StripeDeployment stripeDeployment = task.getStripeDeployment();

        QueryId queryId = stripeDeployment.getContext().getQueryId();
        Exec exec = stripeDeployment.getExec();

        // Setup and register inboxes.
        List<AbstractInbox> stripeInboxes = stripeDeployment.getInboxes();

        for (AbstractInbox inbox : stripeInboxes) {
            inbox.setExec(exec);

            inboxes.put(new InboxKey(queryId, inbox.getEdgeId(), inbox.getStripe()), inbox);
        }

        // Setup executor.
        exec.setup(stripeDeployment.getContext(), this);

        // Start executor.
        exec.advance();
    }

    /**
     * Process incoming batch.
     *
     * @param task Task descriptor.
     */
    private void handleBatchTask(BatchDataTask task) {
        // Locate the inbox.
        QueryId queryId = task.getQueryId();
        int edgeId = task.getEdgeId();
        int stripe = task.getTargetStripe();

        InboxKey inboxKey = new InboxKey(queryId, edgeId, stripe);

        AbstractInbox inbox = inboxes.get(inboxKey);

        // Feed the batch.
        inbox.onBatch(task.getSourceMemberId(), task.getSourceStripe(), task.getSourceThread(), task.getBatch());

        // Continue iteration.
        inbox.getExec().advance();
    }

    /**
     * Try transferring data to user.
     *
     * @param task Task descriptor.
     */
    private void handleRootTask(RootDataTask task) {
        RootExec root = task.getRootExec();

        root.advance();
    }

    @Override
    protected void onStop() {

    }

    /**
     * @return Thread index.
     */
    public int getThread() {
        return thread;
    }
}
