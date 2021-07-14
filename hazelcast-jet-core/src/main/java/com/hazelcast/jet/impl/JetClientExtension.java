package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.clientside.DefaultClientExtension;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.jet.JetInstance;

public class JetClientExtension extends DefaultClientExtension {

    private JetClientInstanceImpl jetClientInstance;

    @Override
    public void afterStart(HazelcastClientInstanceImpl client) {
        super.afterStart(client);
        jetClientInstance = new JetClientInstanceImpl(client);
    }

    @Override
    public JetInstance getJetInstance() {
        return jetClientInstance;
    }
}
