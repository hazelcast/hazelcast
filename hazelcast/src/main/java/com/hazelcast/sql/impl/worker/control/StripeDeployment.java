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

package com.hazelcast.sql.impl.worker.control;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.Outbox;

import java.util.List;

/**
 * Deployment of a single fragment on a single stripe.
 */
public class StripeDeployment {
    /** Executor. */
    private final Exec exec;

    /** Stripe index. */
    private final int stripe;

    /** Thread index. */
    private final int thread;

    /** Inboxes. */
    private final List<AbstractInbox> inboxes;

    /** Outboxes. */
    private final List<Outbox> outboxes;

    /** Query context. */
    private QueryContext ctx;

    /** Parent fragment. */
    private FragmentDeployment fragmentDeployment;

    /** Whether stripe execution is finished. */
    private volatile boolean done;

    public StripeDeployment(Exec exec, int stripe, int thread, List<AbstractInbox> inboxes, List<Outbox> outboxes) {
        this.exec = exec;
        this.stripe = stripe;
        this.thread = thread;
        this.inboxes = inboxes;
        this.outboxes = outboxes;
    }

    public void initialize(QueryContext ctx, FragmentDeployment fragmentDeployment) {
        this.ctx = ctx;
        this.fragmentDeployment = fragmentDeployment;
    }

    public Exec getExec() {
        return exec;
    }

    public int getStripe() {
        return stripe;
    }

    public int getThread() {
        return thread;
    }

    public List<AbstractInbox> getInboxes() {
        return inboxes;
    }

    public List<Outbox> getOutboxes() {
        return outboxes;
    }

    public boolean isDone() {
        return done;
    }

    // TODO: Exception, result, etc.
    public void onDone() {
        done = true;
    }

    public QueryContext getContext() {
        return ctx;
    }

    public FragmentDeployment getFragmentDeployment() {
        return fragmentDeployment;
    }
}
