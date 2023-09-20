/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.List;

public class JdbcJoinParameters implements DataSerializable {

    // The parameters received from NestedLoop
    private String selectQuery;

    private JetJoinInfo joinInfo;

    private List<Expression<?>> projections;

    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    public JdbcJoinParameters() {
    }

    public JdbcJoinParameters(String selectQuery,
                              JetJoinInfo joinInfo,
                              List<Expression<?>> projections) {
        this.selectQuery = selectQuery;
        this.joinInfo = joinInfo;
        this.projections = projections;
    }

    public String getSelectQuery() {
        return selectQuery;
    }

    public JetJoinInfo getJoinInfo() {
        return joinInfo;
    }

    public List<Expression<?>> getProjections() {
        return projections;
    }
  @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(selectQuery);
        out.writeObject(joinInfo);
        out.writeObject(projections);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        selectQuery = in.readString();
        joinInfo = in.readObject();
        projections = in.readObject();
    }
}
