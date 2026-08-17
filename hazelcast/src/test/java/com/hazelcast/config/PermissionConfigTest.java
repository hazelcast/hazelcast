/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.config;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.assertj.core.api.Assertions.assertThat;

class PermissionConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(PermissionConfig.class)
                      .suppress(Warning.NONFINAL_FIELDS)
                      .verify();
    }

    @Test
    public void testCopyConstructor() {
        var config = new PermissionConfig();
        config.setType(PermissionConfig.PermissionType.CONFIG);
        config.setActions(Set.of("ACTION1"));
        config.setDeny(true);
        config.setEndpoints(Set.of("ENDPOINT1", "ENDPOINT2"));
        config.setName("sample name");
        config.setPrincipal("principal");

        var copy = new PermissionConfig(config);

        assertThat(copy).usingRecursiveComparison().isEqualTo(config);
    }
}
