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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlDataLinkStatementTest extends SqlTestSupport {
    private InternalDataLinkService[] dataLinkServices;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(2, null);
    }

    @Before
    public void setUp() throws Exception {
        dataLinkServices = new InternalDataLinkService[instances().length];
        for (int i = 0; i < instances().length; i++) {
            dataLinkServices[i] = getNodeEngineImpl(instances()[i]).getDataLinkService();
        }
    }

    @Test
    public void when_createDataLink_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE DUMMY "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
            assertThat(dataLink.getConfig().getType()).isEqualTo("DUMMY");
            assertThat(dataLink.getConfig().isShared()).isFalse();
            assertThat(dataLink.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createSharedDataLink_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"DUMMY\" "
                + " SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
            assertThat(dataLink.getConfig().getType()).isEqualTo("DUMMY");
            assertThat(dataLink.getConfig().isShared()).isTrue();
            assertThat(dataLink.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createDefaultSharingDataLink_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"DUMMY\" "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
            assertThat(dataLink.getConfig().getType()).isEqualTo("DUMMY");
            assertThat(dataLink.getConfig().isShared()).isTrue();
            assertThat(dataLink.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createDataLink_fullyQualified_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK hazelcast.public." + dlName
                + " TYPE \"DUMMY\" "
                + " OPTIONS ('b' = 'c')");
        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
            assertThat(dataLink.getConfig().getType()).isEqualTo("DUMMY");
            assertThat(dataLink.getConfig().getProperties().get("b")).isEqualTo("c");
        }
    }

    @Test
    public void when_createDataLinkInWrongNameSpace_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK hazelcast.yyy." + dlName
                        + " TYPE \"DUMMY\" "
                        + " NOT SHARED "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("The data link must be created in the \"public\" schema");
    }

    @Test
    public void when_createDataLinkIfAlreadyExists_then_throws() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
        }

        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName
                        + " TYPE \"DUMMY\" "
                        + " NOT SHARED "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link already exists: " + dlName);
    }

    @Test
    public void when_createDataLinkIfAlreadyExistsCreatedByConfig_then_throws() {
        String dlName = randomName();
        DataLinkConfig dataLinkConfig = new DataLinkConfig(dlName)
                .setType("dummy")
                .setProperty("b", "c");
        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            dataLinkService.createConfigDataLink(dataLinkConfig);
        }

        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName
                        + " TYPE \"DUMMY\" "
                        + " NOT SHARED "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Cannot replace a data link created from configuration");
    }

    @Test
    public void when_createDataLinkWithoutType_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("CREATE DATA LINK " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Was expecting one of:" + System.lineSeparator() + "    \"TYPE\" ...");
    }

    @Test
    public void when_createdDataLinkCreatedAfterMappingWithSameName_then_success() {
        String dlName = randomName();
        createMapping(dlName, int.class, int.class);
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");
        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
        }
    }

    @Test
    public void when_createDataLinkWithoutOptions_then_success() {
        String dlName = randomName();

        instance().getSql().execute("CREATE DATA LINK " + dlName  + " TYPE DUMMY SHARED");

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DataLink dataLink = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertThat(dataLink).isNotNull();
        }
    }

    @Test
    public void when_dropDataLink_then_success() {
        String dlName1 = randomName();
        String dlName2 = randomName();

        instance().getSql().execute("CREATE DATA LINK " + dlName1
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");
        instance().getSql().execute("CREATE DATA LINK " + dlName2
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        instance().getSql().execute("DROP DATA LINK " + dlName1);
        instance().getSql().execute("DROP DATA LINK IF EXISTS " + dlName2);

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            assertThatThrownBy(() ->
                    dataLinkService.getAndRetainDataLink(dlName1, DummyDataLink.class))
                    .isInstanceOf(HazelcastException.class)
                    .hasMessageContaining("Data link '" + dlName1 + "' not found");

            assertThatThrownBy(() ->
                    dataLinkService.getAndRetainDataLink(dlName2, DummyDataLink.class))
                    .isInstanceOf(HazelcastException.class)
                    .hasMessageContaining("Data link '" + dlName2 + "' not found");
        }
    }

    @Test
    public void when_dropDataLinkCreatedByConfig_then_throw() {
        String dlName = randomName();

        DataLinkConfig dataLinkConfig = new DataLinkConfig(dlName)
                .setType("dummy")
                .setProperty("b", "c");
        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            dataLinkService.createConfigDataLink(dataLinkConfig);
        }

        assertThatThrownBy(() ->
                instance().getSql().execute("DROP DATA LINK " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("is configured via Config and can't be removed");
    }

    @Test
    public void when_dropNonExistentDataLink_withIfExists_then_success() {
        // this should throw no error
        instance().getSql().execute("DROP DATA LINK IF EXISTS " + randomName());
    }

    @Test
    public void when_dropNonExistentDataLink_withoutIfExists_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().execute("DROP DATA LINK " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data link does not exist");
    }

    @Test
    public void when_createIfNotExists_then_notOverwritten() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        instance().getSql().execute("CREATE DATA LINK IF NOT EXISTS " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('d' = 'e')");

        for (InternalDataLinkService dataLinkService : dataLinkServices) {
            DummyDataLink link = dataLinkService.getAndRetainDataLink(dlName, DummyDataLink.class);
            assertNotNull(link.options().get("b"));
            assertNull(link.options().get("d"));
        }
    }
}
