package com.hazelcast.config.tpc;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class TPCSocketConfig {
    private static final Collection<String> DEFAULT_PORT_DEFINITIONS = Collections.singleton("11000-21000");
    private static final int DEFAULT_RECEIVE_BUFFER_SIZE = 128 * 1024;
    private static final int DEFAULT_SEND_BUFFER_SIZE = 128 * 1024;

    private Collection<String> portDefinitions = DEFAULT_PORT_DEFINITIONS;
    private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
    private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;

    public Collection<String> getPortDefinitions() {
        return portDefinitions;
    }

    public TPCSocketConfig addPortDefinition(@Nonnull String portDefinition) {
        try {
            portDefinitions.add(portDefinition);
        } catch (UnsupportedOperationException e) {
            // since user modified port definitions, remove default range
            portDefinitions = new HashSet<>();
            portDefinitions.add(portDefinition);
        }
        return this;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public TPCSocketConfig setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public TPCSocketConfig setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }
}
