/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.exception.JetDisabledException;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.InternalSqlService;
import com.hazelcast.sql.impl.MissingSqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlServiceIsMissingIfJetIsNotEnabledTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Config config = new Config();
        config.getJetConfig().setEnabled(false);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        InternalSqlService sqlService = (InternalSqlService) hazelcastInstance.getSql();

        assertInstanceOf(MissingSqlService.class, sqlService);

        assertThrows(JetDisabledException.class, sqlService::ensureSqlIsEnabled);
        // HZOLD-3438
        assertThrows(JetDisabledException.class, () -> sqlService.mappingDdl("abxy"));
    }

    @Test
    public void shouldNotDeserializeParametersIfJetIsNotEnabled() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        Config config = new Config();
        config.getJetConfig().setEnabled(false);

        try {
            factory.newHazelcastInstance(config);
            var client = factory.newHazelcastClient();

            ShouldNotBeDeserialized.attemptedDeserialization = false;
            assertThatThrownBy(() ->  client.getSql().execute("select ?", new ShouldNotBeDeserialized()))
                    .isInstanceOf(HazelcastSqlException.class)
                    .hasMessageContaining("The Jet engine is disabled.");
            assertThat(ShouldNotBeDeserialized.attemptedDeserialization)
                    .as("Should not deserialize parameters").isFalse();
        } finally {
            factory.terminateAll();
        }
    }

    private static class ShouldNotBeDeserialized implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        static volatile boolean attemptedDeserialization = false;

        @Serial
        private void readObject(ObjectInputStream in) {
            attemptedDeserialization = true;
            throw new UnsupportedOperationException("Should not be deserialized");
        }
    }
}
