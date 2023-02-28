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

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.impl.DataLinkTestUtil.DummyDataLink;
import com.hazelcast.datalink.impl.InternalDataLinkService;
import com.hazelcast.jet.sql.SqlTestSupport;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlDataLinkStatementTest extends SqlTestSupport {
    private InternalDataLinkService dataLinkService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        dataLinkService = getNodeEngineImpl(instance()).getDataLinkService();
    }

    @Test
    public void when_createDataLink_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                + " OPTIONS ('b' = 'c')");
        DataLink dataLink = dataLinkService.getDataLink(dlName, DummyDataLink.class);
        assertThat(dataLink).isNotNull();
        assertThat(dataLink.getConfig().getClassName()).isEqualTo(DummyDataLink.class.getName());
        assertThat(dataLink.getConfig().getProperties().get("b")).isEqualTo("c");
    }

    @Test
    public void when_createDataLinkIfAlreadyExists_then_throws() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                + " OPTIONS ('b' = 'c')");
        DataLink dataLink = dataLinkService.getDataLink(dlName, DummyDataLink.class);
        assertThat(dataLink).isNotNull();

        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName
                        + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + dlName + "' already exists");
    }

    @Test
    public void when_createDataLinkIfAlreadyExistsCreatedByConfig_then_throws() {
        String dlName = randomName();
        DataLinkConfig dataLinkConfig = new DataLinkConfig(dlName)
                .setClassName(DummyDataLink.class.getName())
                .setProperty("b", "c");
        dataLinkService.createConfigDataLink(dataLinkConfig);

        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName
                        + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + dlName + "' already exists");
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
        String dlName1 = randomName();
        String dlName2 = randomName();

        instance().getSql().execute("CREATE DATA LINK " + dlName1
                + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                + " OPTIONS ('b' = 'c')");
        instance().getSql().execute("CREATE DATA LINK " + dlName2
                + " TYPE \"" + DummyDataLink.class.getName() + "\" "
                + " OPTIONS ('b' = 'c')");

        instance().getSql().execute("DROP DATA LINK " + dlName1);
        instance().getSql().execute("DROP DATA LINK IF EXISTS " + dlName2);

        assertThatThrownBy(() ->
                dataLinkService.getDataLink(dlName1, DummyDataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + dlName1 + "' not found");

        assertThatThrownBy(() ->
                dataLinkService.getDataLink(dlName2, DummyDataLink.class))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link '" + dlName2 + "' not found");
    }

    @Test
    public void when_dropDataLinkCreatedByConfig_then_throw() {
        String dlName = randomName();

        DataLinkConfig dataLinkConfig = new DataLinkConfig(dlName)
                .setClassName(DummyDataLink.class.getName())
                .setProperty("b", "c");
        dataLinkService.createConfigDataLink(dataLinkConfig);

        assertThatThrownBy(() ->
                instance().getSql().execute("DROP DATA LINK " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("is configured via Config and can't be removed");
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
    public void when_dropNonExistentDataLink_withoutIfExists_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("DROP DATA LINK " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link does not exist");
    }
}
