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

package com.hazelcast.jet.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetClientInstanceImplTest extends JetTestSupport {

    @Test
    public void given_singleMapOnMember_when_getDistributedObjectsCalled_then_ReturnedObjectInfo() {
        // Given
        HazelcastInstance member = createHazelcastInstance();
        JetClientInstanceImpl client = (JetClientInstanceImpl) createHazelcastClient().getJet();
        String mapName = randomMapName();
        member.getMap(mapName);

        // When
        List<DistributedObjectInfo> objects = client.getDistributedObjects();

        // Then
        assertFalse(objects.isEmpty());
        DistributedObjectInfo info = objects.stream()
                .filter(i -> mapName.equals(i.getName()))
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(MapService.SERVICE_NAME, info.getServiceName());
    }

    @Test
    public void calculateTotalParts() {
        long jarSize = 10_000_000;
        int partSize = 10_000_000;

        // Member is required to start the client
        createHazelcastInstance();
        JetClientInstanceImpl jetClientInstance = (JetClientInstanceImpl) createHazelcastClient().getJet();

        int totalParts = jetClientInstance.calculateTotalParts(jarSize, partSize);
        assertEquals(1, totalParts);
    }

    @Test
    public void calculatePartSize_when_validProperty() {
        // Member is required to start the client
        createHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "1000");

        HazelcastInstance hazelcastClient = createHazelcastClient(clientConfig);
        JetClientInstanceImpl jetClientInstance = (JetClientInstanceImpl) hazelcastClient.getJet();

        int partSize = jetClientInstance.calculatePartBufferSize();
        assertEquals(1_000, partSize);
    }

    @Test
    public void calculatePartSize_when_invalidProperty() {
        // Member is required to start the client
        createHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.JOB_UPLOAD_PART_SIZE.getName(), "E");
        HazelcastInstance hazelcastClient = createHazelcastClient(clientConfig);

        JetClientInstanceImpl jetClientInstance = (JetClientInstanceImpl) hazelcastClient.getJet();

        assertThrows(NumberFormatException.class, jetClientInstance::calculatePartBufferSize);

    }

    @Test
    public void fileNameWithoutExtension() {
        // Member is required to start the client
        createHazelcastInstance();

        HazelcastInstance hazelcastClient = createHazelcastClient();
        JetClientInstanceImpl jetClientInstance = (JetClientInstanceImpl) hazelcastClient.getJet();

        String expectedFileName = "foo";
        Path jarPath = Paths.get("/mnt/foo.jar");
        String fileNameWithoutExtension = jetClientInstance.getFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);

        Path jarPath1 = Paths.get("/mnt/foo");
        assertThrows(JetException.class, () -> jetClientInstance.getFileNameWithoutExtension(jarPath1));

        jarPath = Paths.get("foo.jar");
        fileNameWithoutExtension = jetClientInstance.getFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);

        Path jarPath2 = Paths.get("foo");
        assertThrows(JetException.class, () -> jetClientInstance.getFileNameWithoutExtension(jarPath2));
    }

    @Test
    public void validateParameterObject_NullJarPath() {
        // Member is required to start the client
        createHazelcastInstance();
        JetClientInstanceImpl jetClientInstance = (JetClientInstanceImpl) createHazelcastClient().getJet();

        SubmitJobParameters parameterObject = new SubmitJobParameters();
        assertThatThrownBy(() -> jetClientInstance.validateParameterObject(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }
}
