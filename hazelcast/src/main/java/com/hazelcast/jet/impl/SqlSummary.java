package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class SqlSummary implements IdentifiedDataSerializable {
    private String query;
    private boolean unbounded;

    public SqlSummary() {
    }

    public SqlSummary(String query, boolean unbounded) {
        this.query = query;
        this.unbounded = unbounded;
    }

    public String getQuery() {
        return query;
    }

    public boolean isUnbounded() {
        return unbounded;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(query);
        out.writeBoolean(unbounded);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        query = in.readString();
        unbounded = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_SUMMARY;
    }

    @Override
    public String toString() {
        return "SqlSummary{" +
                "query='" + query + '\'' +
                ", unbounded=" + unbounded +
                '}';
    }
}
