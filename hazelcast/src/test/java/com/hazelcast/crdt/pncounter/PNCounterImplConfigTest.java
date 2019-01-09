/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.crdt.pncounter;

import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterImplConfigTest extends HazelcastTestSupport {

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenReplicationPeriodIsNegative() {
        new CRDTReplicationConfig().setReplicationPeriodMillis(-200);
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenReplicationPeriodIsZero() {
        new CRDTReplicationConfig().setReplicationPeriodMillis(0);
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenMaxTargetsIsNegative() {
        new CRDTReplicationConfig().setMaxConcurrentReplicationTargets(-200);
    }

    @Test(expected = ConfigurationException.class)
    public void getProxyFailsWhenMaxTargetsIsZero() {
        new CRDTReplicationConfig().setMaxConcurrentReplicationTargets(0);
    }
}
