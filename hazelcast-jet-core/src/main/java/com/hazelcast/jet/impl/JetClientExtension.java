package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.clientside.DefaultClientExtension;
import com.hazelcast.jet.JetInstance;

public class JetClientExtension extends DefaultClientExtension {

    @Override
    public JetInstance getJetInstance() {
        return super.getJetInstance();
    }
}
