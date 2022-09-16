/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.ClusterProperty.INVOCATION_RETRY_PAUSE;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationSlowTest extends CompactSchemaReplicationTestBase {

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringPreparationPhase() {
        doThrow(new RetryableHazelcastException())
                .when(getSchemaService(instance1))
                .onSchemaPreparationRequest(SCHEMA);

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instance2));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            if (instance == instance1) {
                // For instance1, it must fail all the time
                verify(service, atLeast(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS)).onSchemaPreparationRequest(SCHEMA);
            }

            // No-one should call onSchemaAckRequest
            verify(service, never()).onSchemaAckRequest(SCHEMA.getSchemaId());
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringAcknowledgmentPhase() {
        doThrow(new RetryableHazelcastException())
                .when(getSchemaService(instance4))
                .onSchemaAckRequest(SCHEMA.getSchemaId());

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instance1));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            // Everyone should call, apart from the initiator
            if (instance != instance1) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            if (instance == instance4) {
                // For instance4, it must fail all the time
                verify(service, atLeast(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS)).onSchemaAckRequest(SCHEMA.getSchemaId());
            }
        }
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        // Jet prints too many logs during the test
        config.getJetConfig().setEnabled(false);
        config.getProperties().setProperty(INVOCATION_RETRY_PAUSE.getName(), "100");
        return config;
    }
}
