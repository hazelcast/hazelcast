/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.file;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.hadoop.impl.HadoopTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.test.Assertions;
import com.hazelcast.test.HazelcastParametrizedRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParametrizedRunner.class)
public abstract class BaseFileFormatTest extends HadoopTestSupport {

    @Parameter
    public boolean useHadoop;

    protected String currentDir;

    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }

    @Override
    protected boolean useHadoop() {
        return useHadoop;
    }

    @Before
    public void setUp() {
        currentDir = Paths.get(".").toAbsolutePath().normalize().toString();
    }

    @SafeVarargs
    protected final <T> void assertItemsInSource(FileSourceBuilder<T> source, T... items) {
        assertItemsInSource(source, collected -> assertThat(collected).containsOnly(items));
    }

    protected <T> void assertItemsInSource(
            FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        assertItemsInSource(1, source, assertion);
    }

    protected <T> void assertItemsInSource(
            int memberCount, FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .apply(Assertions.assertCollected(assertion));

        HazelcastInstance[] instances = createHazelcastInstances(memberCount);
        try {
            instances[0].getJet().newJob(p).join();
        } finally {
            for (HazelcastInstance instance : instances) {
                instance.shutdown();
            }
        }
    }

    protected void assertJobFailed(
            FileSourceBuilder<?> source,
            Class<? extends Throwable> expectedRootException,
            String expectedMessage
    ) {
        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .writeTo(Sinks.logger());

        HazelcastInstance[] instances = createHazelcastInstances(1);

        try {
            assertThatThrownBy(() -> instances[0].getJet().newJob(p).join())
                    .hasCauseInstanceOf(JetException.class)
                    // can't use hasRootCauseInstanceOf because of mismatch between shaded/non-shaded version
                    .hasStackTraceContaining(expectedRootException.getName())
                    .hasMessageContaining(expectedMessage);
        } finally {
            for (HazelcastInstance instance : instances) {
                instance.shutdown();
            }
        }
    }
}
