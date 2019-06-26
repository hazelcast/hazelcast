package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.worker.control.StripeDeployment;

/**
 * Task to start stripe execution.
 */
public class StartStripeDataTask implements DataTask {
    /** Deployment. */
    private final StripeDeployment stripeDeployment;

    public StartStripeDataTask(StripeDeployment stripeDeployment) {
        this.stripeDeployment = stripeDeployment;
    }

    public StripeDeployment getStripeDeployment() {
        return stripeDeployment;
    }

    @Override
    public int getThread() {
        return stripeDeployment.getThread();
    }
}
