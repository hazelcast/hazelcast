/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
        return JetInitDataSerializerHook.SQL_SUMMARY;
    }

    @Override
    public String toString() {
        return "SqlSummary{" +
                "query='" + query + '\'' +
                ", unbounded=" + unbounded +
                '}';
    }
}
