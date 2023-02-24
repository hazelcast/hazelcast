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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.impl.DataLinkTestUtil.DummyDataLink;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.UpdateSqlResultImpl;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlDataLinkStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void when_createDataLink_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                + " OPTIONS ('b' = 'c')");
        DataLink dataLink = getNodeEngineImpl(instance()).getDataLinkService().getDataLink(dlName);
        assertThat(dataLink).isNotNull();
        assertThat(dataLink.getConfig().getClassName()).isEqualTo(DummyDataLink.class.getName());
        assertThat(dataLink.getConfig().getProperties().get("b")).isEqualTo("c");
    }

    @Test
    public void when_createDataLinkWithoutType_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Was expecting one of:\n    \"TYPE\" ...");
    }

    @Test
    public void when_createDataLinkWithoutOptions_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName
                        + " TYPE \"" + DummyDataLink.class.getName() + "\" "))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Was expecting:\n    \"OPTIONS\" ...");
    }

    @Test
    public void when_dropDataLink_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                + " OPTIONS ('b' = 'c')");

        instance().getSql().execute("DROP DATA LINK " + dlName);

        assertThatThrownBy(() ->
                getNodeEngineImpl(instance()).getDataLinkService().getDataLink(dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + dlName + "' not found");
    }

    @Test
    public void when_dropNonExistentDataLink_withIfExists_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("DROP DATA LINK IF EXISTS " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + dlName + "' not found");
    }

    @Test
    public void when_dropNonExistentDataLink_woIfExists_then_throws() {
        String dlName = randomName();
        SqlResult result = instance().getSql().execute("DROP DATA LINK " + dlName);
        assertThat(result).isInstanceOf(UpdateSqlResultImpl.class);
        UpdateSqlResultImpl updateSqlResult = (UpdateSqlResultImpl) result;
        assertThat(updateSqlResult.updateCount()).isZero();
    }
}
