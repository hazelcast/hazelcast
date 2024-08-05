/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.ClusterProperty.INVOCATION_RETRY_PAUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationSlowTest extends CompactSchemaReplicationTestBase {

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringPreparationPhase() {
        StubMemberSchemaService stubMemberSchemaService = (StubMemberSchemaService) stubbedSchemaService;
        stubMemberSchemaService.setThrowOnSchemaPreparationRequest(true);

        // member at index 0 will always throw retryable exception
        // in the preparation phase, others will work fine.
        int stubbedMemberIndex = 0;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : createSpiedMemberSchemaService());
        HazelcastInstance stubbed = instances[stubbedMemberIndex];

        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(instances[1]));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            StubMemberSchemaService stubService = (StubMemberSchemaService) service;

            if (instance == stubbed) {
                // For stubbed member, it must fail all the time
                assertThat(stubService.schemaPreparationRequestCounter)
                        .isGreaterThanOrEqualTo(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS);
            }

            // No-one should call onSchemaAckRequest
            assertNotEquals(SchemaReplicationStatus.REPLICATED, service.getReplicator().getReplicationStatus(SCHEMA));
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringAcknowledgmentPhase() {
        StubMemberSchemaService stubMemberSchemaService = (StubMemberSchemaService) stubbedSchemaService;
        stubMemberSchemaService.setThrowOnSchemaAckRequest(true);

        // member at index 3 will always throw retryable exception
        // in the acknowledgment phase, others will work fine.
        int stubbedMemberIndex = 3;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : createSpiedMemberSchemaService());
        HazelcastInstance stubbed = instances[stubbedMemberIndex];

        HazelcastInstance initiator = instances[0];
        assertThrows(HazelcastSerializationException.class, () -> fillMapUsing(initiator));

        for (HazelcastInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            StubMemberSchemaService stubService = (StubMemberSchemaService) service;

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != initiator) {
                assertThat(stubService.getSchemaPreparationRequestCounter())
                        .isPositive();
            }

            if (instance == stubbed) {
                // For stubbed member, it must fail all the time
                assertThat(stubService.getSchemaAckRequestCounter())
                        .isGreaterThanOrEqualTo(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS);
            }
        }
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        config.getProperties().setProperty(INVOCATION_RETRY_PAUSE.getName(), String.valueOf(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS));
        return config;
    }

    // Mockito.spy() call fails on Zing. So create a test stub
    @Override
    protected StubMemberSchemaService createSpiedMemberSchemaService() {
        return new StubMemberSchemaService();
    }

    // Test stub for Zing
    protected static class StubMemberSchemaService extends MemberSchemaService {
        // Flag that indicates whether exception should be thrown
        private boolean throwOnSchemaPreparationRequest;

        // Counts number of onSchemaPreparationRequest() calls
        private int schemaPreparationRequestCounter;

        // Flag that indicates whether exception should be thrown
        private boolean throwOnSchemaAckRequest;

        // Counts number of onSchemaAckRequest() calls
        private int schemaAckRequestCounter;

        public void setThrowOnSchemaPreparationRequest(boolean throwOnSchemaPreparationRequest) {
            this.throwOnSchemaPreparationRequest = throwOnSchemaPreparationRequest;
        }

        public void setThrowOnSchemaAckRequest(boolean throwOnSchemaAckRequest) {
            this.throwOnSchemaAckRequest = throwOnSchemaAckRequest;
        }

        public int getSchemaAckRequestCounter() {
            return schemaAckRequestCounter;
        }

        public int getSchemaPreparationRequestCounter() {
            return schemaPreparationRequestCounter;
        }

        @Override
        public void onSchemaPreparationRequest(Schema schema) {
            if (schema.equals(SCHEMA)) {
                schemaPreparationRequestCounter++;
            }

            if (throwOnSchemaPreparationRequest) {
                throw new RetryableHazelcastException();
            }
            super.onSchemaPreparationRequest(schema);
        }

        @Override
        public void onSchemaAckRequest(long schemaId) {
            if (schemaId == SCHEMA.getSchemaId()) {
                schemaAckRequestCounter++;
            }

            if (throwOnSchemaAckRequest) {
                throw new RetryableHazelcastException();
            }
            super.onSchemaAckRequest(schemaId);
        }
    }
}
