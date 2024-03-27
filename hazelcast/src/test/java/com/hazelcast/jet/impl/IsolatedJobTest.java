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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.jet.core.processor.Processors.noopP;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category(QuickTest.class)
public class IsolatedJobTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_normal() {
        assertThatThrownBy(() ->
                client().getJet().newJobBuilder(new DAG().vertex(new Vertex("test", noopP())))
                        .withMemberSelector(member -> true)
                        .start()
                        .join())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("The Isolated Jobs feature is only available in Hazelcast Enterprise Edition.");
    }

    @Test
    public void test_light() {
        assertThatThrownBy(() ->
                client().getJet().newJobBuilder(new DAG().vertex(new Vertex("test", noopP())))
                        .withMemberSelector(member -> true)
                        .asLightJob()
                        .start()
                        .join())
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("The Isolated Jobs feature is only available in Hazelcast Enterprise Edition.");
    }
}
