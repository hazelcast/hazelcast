package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.worker.control.StripeDeployment;

public class StartStripeDataTask implements DataTask {
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
