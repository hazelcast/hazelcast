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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlDataStoreStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void when_createDataStore_then_notImplemented() {
        assertThatThrownBy(() -> instance().getSql().execute("CREATE DATA STORE a TYPE Kafka OPTIONS ()"))
                .hasMessageContaining("CREATE DATA STORE is not implemented yet");
    }

    @Test
    public void when_dropDataStore_then_notImplemented() {
        assertThatThrownBy(() -> instance().getSql().execute("DROP DATA STORE a"))
                .hasMessageContaining("CREATE DATA STORE is not implemented yet");
    }
}
