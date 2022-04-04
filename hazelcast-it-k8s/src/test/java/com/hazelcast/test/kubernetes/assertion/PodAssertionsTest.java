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

package com.hazelcast.test.kubernetes.assertion;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class PodAssertionsTest {

    @Test
    public void should_confirm_pod_is_running() {
        Pod pod = somePodWithStatus("Running");
        PodAssertions.assertThatPod(pod).isRunning();
    }

    private Pod somePodWithStatus(String status) {
        return new PodBuilder()
                .withStatus(new PodStatusBuilder().withPhase(status).build())
                .withNewMetadata().withName("pod-name").endMetadata()
                .build();
    }

    @Test
    public void should_confirm_pod_is_NOT_running() {
        Pod pod = somePodWithStatus("Pending");
        Assertions.assertThatThrownBy(() -> PodAssertions.assertThatPod(pod).isRunning())
                .isInstanceOf(AssertionError.class)
                .hasMessage("Pod 'pod-name' should be 'Running' but is 'Pending'");
    }
}
