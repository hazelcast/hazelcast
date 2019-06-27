package com.hazelcast.internal.query.worker.data;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.internal.query.worker.AbstractWorker;
import com.hazelcast.internal.query.worker.control.StripeDeployment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataWorker extends AbstractWorker<DataTask> {
    /** Stripe not mapped to any thread yet. */
    public static final int UNMAPPED_STRIPE = -1;

    private final DataThreadPool dataPool;
    private final Map<InboxKey, AbstractInbox> inboxes = new HashMap<>();
    private final int thread;

    public DataWorker(DataThreadPool dataPool, int thread) {
        this.dataPool = dataPool;
        this.thread = thread;
    }

    @Override
    protected void executeTask(DataTask task) {
        if (task instanceof StartStripeDataTask)
            handleStartStripe((StartStripeDataTask)task);
        else if (task instanceof BatchDataTask)
            handleBatch((BatchDataTask)task);
        else if (task instanceof RootDataTask)
            handleRoot((RootDataTask)task);
    }

    private void handleStartStripe(StartStripeDataTask task) {
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

    private void handleBatch(BatchDataTask task) {
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

    private void handleRoot(RootDataTask task) {
        RootExec root = task.getRootExec();

        root.advance();
    }

    @Override
    protected void onStop() {
        // TODO: Handle node stop
    }

    public int getThread() {
        return thread;
    }

    public DataThreadPool getDataPool() {
        return dataPool;
    }
}
