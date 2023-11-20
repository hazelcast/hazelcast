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

package com.hazelcast.jet.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.exception.JetDisabledException;
import com.hazelcast.sql.impl.InternalSqlService;
import com.hazelcast.sql.impl.MissingSqlService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlServiceIsMissingIfJetIsNotEnabledTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Config config = new Config();
        config.getJetConfig().setEnabled(false);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        InternalSqlService sqlService = (InternalSqlService) hazelcastInstance.getSql();

        assertInstanceOf(MissingSqlService.class, sqlService);

        // HZ-3438
        assertThrows(JetDisabledException.class, () -> sqlService.mappingDdl("abxy"));
    }
}
