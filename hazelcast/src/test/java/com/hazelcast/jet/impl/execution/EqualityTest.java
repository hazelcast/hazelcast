/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.test.HazelcastParallelClassRunner;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
public class EqualityTest {

    @Test
    public void testEqualsAndHashCode_whenBroadcastKeyReference() {
        EqualsVerifier.forClass(BroadcastKey.class)
                      .usingGetClass()
                      .verify();
    }

    @Test
    public void testEqualsAndHashCode_whenSnapshotBarrier() {
        EqualsVerifier.forClass(SnapshotBarrier.class)
                      .usingGetClass()
                      .verify();
    }


}
