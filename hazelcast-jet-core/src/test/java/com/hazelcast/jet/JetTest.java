/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import org.junit.Test;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static org.junit.Assert.assertEquals;

public class JetTest extends JetTestSupport {

    @Test
    public void when_defaultMapConfig_then_notUsed() {
        // When
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapConfig("default")
                .setTimeToLiveSeconds(MapConfig.DEFAULT_TTL_SECONDS + 1);
        JetInstance instance = createJetMember(config);

        try {
            // Then
            int actualTTL = instance.getConfig().getHazelcastConfig().findMapConfig(INTERNAL_JET_OBJECTS_PREFIX + "fooMap")
                                    .getTimeToLiveSeconds();
            assertEquals(MapConfig.DEFAULT_TTL_SECONDS, actualTTL);
        } finally {
            shutdownFactory();
        }
    }

}
