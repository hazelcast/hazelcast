package com.hazelcast.client.config;

import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Contains the SQL configuration for the client.
 */
public class ClientSqlConfig {
    private ClientSqlResubmissionMode resubmissionMode = ClientSqlResubmissionMode.NEVER;

    @Nonnull
    public ClientSqlConfig setSqlResubmissionMode(ClientSqlResubmissionMode resubmissionMode) {
        Preconditions.checkNotNull(resubmissionMode, "resubmissionMode");
        this.resubmissionMode = resubmissionMode;
        return this;
    }

    public ClientSqlResubmissionMode getResubmissionMode() {
        return resubmissionMode;
    }

    @Override
    public String toString() {
        return "ClientSqlConfig{" +
                "resubmissionMode=" + resubmissionMode +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientSqlConfig that = (ClientSqlConfig) o;
        return resubmissionMode == that.resubmissionMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resubmissionMode);
    }
}
