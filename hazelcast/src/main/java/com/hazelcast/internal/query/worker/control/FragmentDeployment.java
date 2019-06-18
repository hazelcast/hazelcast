package com.hazelcast.internal.query.worker.control;

import java.util.List;

public class FragmentDeployment {

    private final List<StripeDeployment> stripes;

    public FragmentDeployment(List<StripeDeployment> stripes) {
        this.stripes = stripes;
    }

    public List<StripeDeployment> getStripes() {
        return stripes;
    }
}
