/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.sql.HazelcastSqlException;
import org.junit.Test;

import static com.hazelcast.jet.impl.util.Util.JET_IS_DISABLED_MESSAGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlJetDisabledTest extends JetTestSupport {

    @Test
    public void when_jetDisabled_and_usingClient_then_sqlThrowsException() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        createHazelcastInstance(config);

        HazelcastInstance client = createHazelcastClient();

        assertThatThrownBy(() -> client.getSql().execute("SELECT * FROM TABLE(GENERATE_SERIES(1, 1))").iterator().next())
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining(JET_IS_DISABLED_MESSAGE);
    }
}
