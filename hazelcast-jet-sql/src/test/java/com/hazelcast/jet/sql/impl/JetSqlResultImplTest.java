/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.junit.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JetSqlResultImplTest extends JetTestSupport {

    @Test
    // test for https://github.com/hazelcast/hazelcast-jet/issues/2697
    public void when_closed_then_iteratorFails() {
        JetInstance inst = createJetMember();
        TestBatchSqlConnector.create(inst.getSql(), "m", 1);
        SqlResult sqlResult = inst.getSql().execute("select * from m");
        sqlResult.close();
        Iterator<SqlRow> iterator = sqlResult.iterator();
        assertThatThrownBy(() -> iterator.hasNext());
    }
}
