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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationTest extends CompactSchemaReplicationTestBase {
    @Test
    public void testSchemaReplication() {
        // We won't stub any of the schema services.
        // We will just assert call counts.
        setupInstances(index -> spy(new MemberSchemaService()));

        HazelcastInstance initiator = instances[0];
        fillMapUsing(initiator);

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            SchemaReplicator replicator = service.getReplicator();

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Everyone should call onSchemaAckRequest, apart from the initiator
            if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
            }

            // It should be replicated everywhere
            assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(SCHEMA));
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrows_duringPreparationPhase() {
        MemberSchemaService stubbedSchemaService = spy(new MemberSchemaService());
        doThrow(new RuntimeException())
                .when(stubbedSchemaService)
                .onSchemaPreparationRequest(any(Schema.class));

        // First member will always throw non-retryable exception
        // in the preparation phase, others will work fine.
        setupInstances(index -> index == 0 ? stubbedSchemaService : spy(new MemberSchemaService()));

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instances[1]));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            // Some members might call onSchemaPreparationRequest
            verify(service, atMostOnce()).onSchemaPreparationRequest(SCHEMA);

            // No-one should call onSchemaAckRequest
            verify(service, never()).onSchemaAckRequest(SCHEMA.getSchemaId());
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionForSomeTime_duringPreparationPhase() {
        MemberSchemaService stubbedSchemaService = spy(new MemberSchemaService());
        doThrow(new RetryableHazelcastException()) // Throw once
                .doCallRealMethod() // Then succeed
                .when(stubbedSchemaService)
                .onSchemaPreparationRequest(SCHEMA);

        // Third member will throw retryable exception and then continue working
        // in the preparation phase, others will work fine.
        int stubbedMemberIndex = 2;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : spy(new MemberSchemaService()));
        HazelcastInstance stubbed = instances[stubbedMemberIndex];

        HazelcastInstance initiator = instances[3];
        fillMapUsing(initiator);

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            SchemaReplicator replicator = service.getReplicator();

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance == stubbed) {
                // For stubbed member, it must at least fail + succeed
                verify(service, atLeast(2)).onSchemaPreparationRequest(SCHEMA);
            } else if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Everyone should call onSchemaAckRequest it, apart from the initiator
            if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
            }

            // It should be replicated everywhere
            assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(SCHEMA));
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrows_duringAcknowledgmentPhase() {
        MemberSchemaService stubbedSchemaService = spy(new MemberSchemaService());
        doThrow(new RuntimeException())
                .when(stubbedSchemaService)
                .onSchemaAckRequest(anyLong());

        // Fourth member will always throw non-retryable exception
        // in the acknowledgment phase, others will work fine.
        setupInstances(index -> index == 3 ? stubbedSchemaService : spy(new MemberSchemaService()));

        HazelcastInstance initiator = instances[0];
        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(initiator));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Some members might call onSchemaAckRequest
            verify(service, atMostOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionForSomeTime_duringAcknowledgmentPhase() {
        MemberSchemaService stubbedSchemaService = spy(new MemberSchemaService());
        doThrow(new RetryableHazelcastException()) // Throw once
                .doCallRealMethod() // Then succeed
                .when(stubbedSchemaService)
                .onSchemaAckRequest(SCHEMA.getSchemaId());

        // Third member will throw retryable exception and then continue working
        // in the preparation phase, others will work fine.
        int stubbedMemberIndex = 1;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : spy(new MemberSchemaService()));
        HazelcastInstance stubbed = instances[stubbedMemberIndex];

        HazelcastInstance initiator = instances[2];
        fillMapUsing(initiator);

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            SchemaReplicator replicator = service.getReplicator();

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Everyone should call onSchemaAckRequest, apart from the initiator
            if (instance == stubbed) {
                // For stubbed member, it must at least fail + succeed
                verify(service, atLeast(2)).onSchemaAckRequest(SCHEMA.getSchemaId());
            } else if (instance != initiator) {
                verify(service, atLeastOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
            }

            // It should be replicated everywhere
            assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(SCHEMA));
        }
    }
}
