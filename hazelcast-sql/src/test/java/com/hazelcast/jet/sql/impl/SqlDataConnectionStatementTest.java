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

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.impl.DataConnectionTestUtil.DummyDataConnection;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlDataConnectionStatementTest extends SqlTestSupport {
    private InternalDataConnectionService[] dataConnectionServices;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(2, null);
    }

    @Before
    public void setUp() throws Exception {
        dataConnectionServices = new InternalDataConnectionService[instances().length];
        for (int i = 0; i < instances().length; i++) {
            dataConnectionServices[i] = getNodeEngineImpl(instances()[i]).getDataConnectionService();
        }
    }

    @Test
    public void when_createDataConnection_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA CONNECTION " + dlName
                + " TYPE DUMMY "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
            assertThat(dataConnection.getConfig().getType()).isEqualTo("dummy");
            assertThat(dataConnection.getConfig().isShared()).isFalse();
            assertThat(dataConnection.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createDataConnectionWithCaseInsensitiveName_then_success() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA CONNECTION " + dlName
                + " TYPE DuMMy "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
            assertThat(dataConnection.getConfig().getType()).isEqualTo("dummy");
            assertThat(dataConnection.getConfig().isShared()).isFalse();
            assertThat(dataConnection.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createDataConnectionWithWrongType_then_throws() {
        String dlName = randomName() + "_shouldNotExist";
        assertThatThrownBy(() -> instance().getSql().execute("CREATE DATA CONNECTION " + dlName
                + " TYPE DUMMIES " // <-- DUMMIES is wrong type
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')"))
                .hasMessageContaining("Data connection type 'DUMMIES' is not known");

        assertRowsAnyOrder("SELECT * FROM information_schema.dataconnections " +
                "WHERE name = '" + dlName + "'", Collections.emptyList());
    }

    @Test
    public void when_createSharedDataConnection_then_success() {
        String dlName = randomName();
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName
                + " TYPE \"DUMMY\" "
                + " SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
            assertThat(dataConnection.getConfig().getType()).isEqualTo("dummy");
            assertThat(dataConnection.getConfig().isShared()).isTrue();
            assertThat(dataConnection.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createDefaultSharingDataConnection_then_success() {
        String dlName = randomName();
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName
                + " TYPE \"DUMMY\" "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
            assertThat(dataConnection.getConfig().getType()).isEqualTo("dummy");
            assertThat(dataConnection.getConfig().isShared()).isTrue();
            assertThat(dataConnection.getConfig().getProperties()).containsEntry("b", "c");
        }
    }

    @Test
    public void when_createDataConnection_fullyQualified_then_success() {
        String dlName = randomName();
        instance().getSql().executeUpdate("CREATE DATA CONNECTION hazelcast.public." + dlName
                + " TYPE \"DUMMY\" "
                + " OPTIONS ('b' = 'c')");
        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
            assertThat(dataConnection.getConfig().getType()).isEqualTo("dummy");
            assertThat(dataConnection.getConfig().getProperties().get("b")).isEqualTo("c");
        }
    }

    @Test
    public void when_createDataConnectionInWrongNameSpace_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().executeUpdate("CREATE DATA CONNECTION hazelcast.yyy." + dlName
                        + " TYPE \"DUMMY\" "
                        + " NOT SHARED "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("The data connection must be created in the \"public\" schema");
    }

    @Test
    public void when_createDataConnectionIfAlreadyExists_then_throws() {
        String dlName = randomName();
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
        }

        assertThatThrownBy(() ->
                instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName
                        + " TYPE \"DUMMY\" "
                        + " NOT SHARED "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data connection already exists: " + dlName);
    }

    @Test
    public void when_createDataConnectionIfAlreadyExistsCreatedByConfig_then_throws() {
        String dlName = randomName();
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig(dlName)
                .setType("dummy")
                .setProperty("b", "c");
        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            dataConnectionService.createConfigDataConnection(dataConnectionConfig);
        }

        assertThatThrownBy(() ->
                instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName
                        + " TYPE \"DUMMY\" "
                        + " NOT SHARED "
                        + " OPTIONS ('b' = 'c')"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Cannot replace a data connection created from configuration");
    }

    @Test
    public void when_createDataConnectionWithoutType_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Was expecting one of:" + System.lineSeparator() + "    \"TYPE\" ...");
    }

    @Test
    public void when_createdDataConnectionCreatedAfterMappingWithSameName_then_success() {
        String dlName = randomName();
        createMapping(dlName, int.class, int.class);
        instance().getSql().execute("CREATE DATA CONNECTION " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");
        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
        }
    }

    @Test
    public void when_createDataConnectionWithoutOptions_then_success() {
        String dlName = randomName();

        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName + " TYPE DUMMY SHARED");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DataConnection dataConnection = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertThat(dataConnection).isNotNull();
        }
    }

    @Test
    public void when_dropDataConnection_then_success() {
        String dlName1 = randomName();
        String dlName2 = randomName();

        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName1
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName2
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        instance().getSql().execute("DROP DATA CONNECTION " + dlName1);
        instance().getSql().execute("DROP DATA CONNECTION IF EXISTS " + dlName2);

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            assertThatThrownBy(() ->
                    dataConnectionService.getAndRetainDataConnection(dlName1, DummyDataConnection.class))
                    .isInstanceOf(HazelcastException.class)
                    .hasMessageContaining("Data connection '" + dlName1 + "' not found");

            assertThatThrownBy(() ->
                    dataConnectionService.getAndRetainDataConnection(dlName2, DummyDataConnection.class))
                    .isInstanceOf(HazelcastException.class)
                    .hasMessageContaining("Data connection '" + dlName2 + "' not found");
        }
    }

    @Test
    public void when_dropDataConnectionCreatedByConfig_then_throw() {
        String dlName = randomName();

        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig(dlName)
                .setType("dummy")
                .setProperty("b", "c");
        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            dataConnectionService.createConfigDataConnection(dataConnectionConfig);
        }

        assertThatThrownBy(() ->
                instance().getSql().executeUpdate("DROP DATA CONNECTION " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("is configured via Config and can't be removed");
    }

    @Test
    public void when_dropNonExistentDataConnection_withIfExists_then_success() {
        // this should throw no error
        instance().getSql().executeUpdate("DROP DATA CONNECTION IF EXISTS " + randomName());
    }

    @Test
    public void when_dropNonExistentDataConnection_withoutIfExists_then_throws() {
        String dlName = randomName();
        assertThatThrownBy(() ->
                instance().getSql().executeUpdate("DROP DATA CONNECTION " + dlName))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Data connection does not exist");
    }

    @Test
    public void when_createIfNotExists_then_notOverwritten() {
        String dlName = randomName();
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('b' = 'c')");

        instance().getSql().executeUpdate("CREATE DATA CONNECTION IF NOT EXISTS " + dlName
                + " TYPE \"DUMMY\" "
                + " NOT SHARED "
                + " OPTIONS ('d' = 'e')");

        for (InternalDataConnectionService dataConnectionService : dataConnectionServices) {
            DummyDataConnection link = dataConnectionService.getAndRetainDataConnection(dlName, DummyDataConnection.class);
            assertNotNull(link.options().get("b"));
            assertNull(link.options().get("d"));
        }
    }
}
