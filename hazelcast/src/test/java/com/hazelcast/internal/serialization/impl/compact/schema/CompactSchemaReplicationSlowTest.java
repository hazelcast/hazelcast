/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationSlowTest extends CompactSchemaReplicationTestBase {

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringPreparationPhase() {
        MemberSchemaService stubbedSchemaService = spy(new MemberSchemaService());
        doThrow(new RetryableHazelcastException())
                .when(stubbedSchemaService)
                .onSchemaPreparationRequest(SCHEMA);

        // First member will always throw retryable exception
        // in the preparation phase, others will work fine.
        int stubbedMemberIndex = 0;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : spy(new MemberSchemaService()));
        HazelcastInstance stubbed = instances[stubbedMemberIndex];

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instances[1]));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            if (instance == stubbed) {
                // For stubbed member, it must fail all the time
                verify(service, atLeast(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS)).onSchemaPreparationRequest(SCHEMA);
            }

            // No-one should call onSchemaAckRequest
            verify(service, never()).onSchemaAckRequest(SCHEMA.getSchemaId());
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringAcknowledgmentPhase() {
        MemberSchemaService stubbedSchemaService = spy(new MemberSchemaService());
        doThrow(new RetryableHazelcastException())
                .when(stubbedSchemaService)
                .onSchemaAckRequest(SCHEMA.getSchemaId());

        // Fourth member will always throw retryable exception
        // in the acknowledgment phase, others will work fine.
        int stubbedMemberIndex = 3;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : spy(new MemberSchemaService()));
        HazelcastInstance stubbed = instances[stubbedMemberIndex];

        HazelcastInstance initiator = instances[0];
        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(initiator));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            if (instance == stubbed) {
                // For stubbed member, it must fail all the time
                verify(service, atLeast(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS)).onSchemaAckRequest(SCHEMA.getSchemaId());
            }
        }
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        config.getProperties().setProperty(INVOCATION_RETRY_PAUSE.getName(), "100");
        return config;
    }
}
