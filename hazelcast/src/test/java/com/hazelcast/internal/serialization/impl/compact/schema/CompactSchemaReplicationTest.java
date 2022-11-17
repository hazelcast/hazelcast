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
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationTest extends CompactSchemaReplicationTestBase {
    @Test
    public void testSchemaReplication() {
        fillMapUsing(instance1);

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            SchemaReplicator replicator = service.getReplicator();

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != instance1) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Everyone should call onSchemaAckRequest, apart from the initiator
            if (instance != instance1) {
                verify(service, atLeastOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
            }

            // It should be replicated everywhere
            assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(SCHEMA));
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrows_duringPreparationPhase() {
        doThrow(new RuntimeException())
                .when(getSchemaService(instance1))
                .onSchemaPreparationRequest(any(Schema.class));

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instance2));

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
        doThrow(new RetryableHazelcastException()) // Throw once
                .doCallRealMethod() // Then succeed
                .when(getSchemaService(instance3))
                .onSchemaPreparationRequest(SCHEMA);

        fillMapUsing(instance4);

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            SchemaReplicator replicator = service.getReplicator();

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance == instance3) {
                // For instance3, it must at least fail + succeed
                verify(service, atLeast(2)).onSchemaPreparationRequest(SCHEMA);
            } else if (instance != instance4) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Everyone should call onSchemaAckRequest it, apart from the initiator
            if (instance != instance4) {
                verify(service, atLeastOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
            }

            // It should be replicated everywhere
            assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(SCHEMA));
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrows_duringAcknowledgmentPhase() {
        doThrow(new RuntimeException())
                .when(getSchemaService(instance4))
                .onSchemaAckRequest(anyLong());

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instance1));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != instance1) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Some members might call onSchemaAckRequest
            verify(service, atMostOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionForSomeTime_duringAcknowledgmentPhase() {
        doThrow(new RetryableHazelcastException()) // Throw once
                .doCallRealMethod() // Then succeed
                .when(getSchemaService(instance2))
                .onSchemaAckRequest(SCHEMA.getSchemaId());

        fillMapUsing(instance3);

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            SchemaReplicator replicator = service.getReplicator();

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != instance3) {
                verify(service, atLeastOnce()).onSchemaPreparationRequest(SCHEMA);
            }

            // Everyone should call onSchemaAckRequest, apart from the initiator
            if (instance == instance2) {
                // For instance2, it must at least fail + succeed
                verify(service, atLeast(2)).onSchemaAckRequest(SCHEMA.getSchemaId());
            } else if (instance != instance3) {
                verify(service, atLeastOnce()).onSchemaAckRequest(SCHEMA.getSchemaId());
            }

            // It should be replicated everywhere
            assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(SCHEMA));
        }
    }
}
