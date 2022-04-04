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
import org.assertj.core.api.AbstractAssert;

public class PodAssertions extends AbstractAssert<PodAssertions, Pod> {

    private static final String RUNNING = "Running";

    public PodAssertions(Pod pod) {
        super(pod, PodAssertions.class);
    }

    public static PodAssertions assertThatPod(Pod actual) {
        return new PodAssertions(actual);
    }

    public PodAssertions isRunning() {
        isNotNull();
        String actualPhase = actual.getStatus().getPhase();
        if (!actualPhase.equals(RUNNING)) {
            failWithMessage("Pod '%s' should be '%s' but is '%s'",
                    actual.getMetadata().getName(), RUNNING, actualPhase);
        }
        return this;
    }
}
