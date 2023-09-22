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

package com.hazelcast.jet.config;

import com.google.common.base.Defaults;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobConfigTest extends JetTestSupport {

    @Test
    public void when_setName_thenReturnsName() {
        // When
        JobConfig config = new JobConfig();
        String name = "myJobName";
        config.setName(name);

        // Then
        assertEquals(name, config.getName());
    }

    @Test
    public void when_enableSplitBrainProtection_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setSplitBrainProtection(true);

        // Then
        assertTrue(config.isSplitBrainProtectionEnabled());
    }

    @Test
    public void when_enableAutoScaling_thenReturnsEnabled() {
        // When
        JobConfig config = new JobConfig();
        config.setAutoScaling(true);

        // Then
        assertTrue(config.isAutoScaling());
    }

    @Test
    public void when_setProcessingGuarantee_thenReturnsProcessingGuarantee() {
        // When
        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);

        // Then
        assertEquals(EXACTLY_ONCE, config.getProcessingGuarantee());
    }

    @Test
    public void when_setSnapshotIntervalMillis_thenReturnsSnapshotIntervalMillis() {
        // When
        JobConfig config = new JobConfig();
        config.setSnapshotIntervalMillis(50);

        // Then
        assertEquals(50, config.getSnapshotIntervalMillis());
    }

    @Test
    public void when_losslessRestartEnabled_then_openSourceMemberDoesNotStart() {
        // When
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setLosslessRestartEnabled(true);

        // Then
        Assert.assertThrows("Lossless Restart requires Hazelcast Enterprise Edition", IllegalStateException.class,
                () -> createHazelcastInstance(config));
    }

    @Test
    public void when_registerSerializerTwice_then_fails() {
        // Given
        JobConfig config = new JobConfig();
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // When
        // Then
        Assert.assertThrows("Serializer for class java.lang.Object already registered", IllegalArgumentException.class,
                () -> config.registerSerializer(Object.class, ObjectSerializer.class));
    }

    @Test
    public void when_registerSerializer() {
        // Given
        JobConfig config = new JobConfig();

        // When
        config.registerSerializer(Object.class, ObjectSerializer.class);

        // Then
        Map<String, String> serializerConfigs = config.getSerializerConfigs();
        assertThat(serializerConfigs.entrySet()).hasSize(1);
        assertThat(serializerConfigs.keySet()).contains(Object.class.getName());
        assertThat(serializerConfigs.values()).contains(ObjectSerializer.class.getName());
    }

    @Test
    public void when_default_then_suspendOnFailureDisabled() {
        // Given
        JobConfig config = new JobConfig();

        // Then
        assertThat(config.isSuspendOnFailure()).isEqualTo(FALSE);
    }

    @Test
    public void when_suspendOnFailureSet_then_suspendOnFailureIsReturned() {
        // Given
        JobConfig config = new JobConfig();

        // When
        config.setSuspendOnFailure(true);

        // Then
        assertThat(config.isSuspendOnFailure()).isEqualTo(TRUE);
    }

    @Test
    public void addCustomClasspath() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspath("test", "url1");
        jobConfig.addCustomClasspath("test", "url2");

        assertThat(jobConfig.getCustomClassPaths()).containsValue(
                newArrayList("url1", "url2")
        );
    }

    @Test
    public void test_jobConfigForLightJob() {
        HazelcastInstance inst = createHazelcastInstance();
        DAG dag = new DAG();

        JobConfig configWithName = new JobConfig();
        configWithName.setName("foo");
        assertThatThrownBy(() -> inst.getJet().newLightJob(dag, configWithName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for light jobs");

        JobConfig configWithResource = new JobConfig();
        configWithResource.addClass(JobConfigTest.class);
        assertThatThrownBy(() -> inst.getJet().newLightJob(dag, configWithResource))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for light jobs");

        JobConfig configWithGuarantee = new JobConfig();
        configWithGuarantee.setProcessingGuarantee(EXACTLY_ONCE);
        assertThatThrownBy(() -> inst.getJet().newLightJob(dag, configWithGuarantee))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for light jobs");

        JobConfig configWithClassLoaderFactory = new JobConfig();
        configWithClassLoaderFactory.setClassLoaderFactory(new JobClassLoaderFactory() {
            @Nonnull @Override
            public ClassLoader getJobClassLoader() {
                throw new UnsupportedOperationException();
            }
        });
        assertThatThrownBy(() -> inst.getJet().newLightJob(dag, configWithClassLoaderFactory))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for light jobs");

        JobConfig configWithInitialSnapshot = new JobConfig();
        configWithInitialSnapshot.setInitialSnapshotName("foo");
        assertThatThrownBy(() -> inst.getJet().newLightJob(dag, configWithInitialSnapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for light jobs");
    }

    @Test
    public void addCustomClasspaths() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspaths("test", newArrayList("url1", "url2"));
        jobConfig.addCustomClasspath("test", "url3");

        assertThat(jobConfig.getCustomClassPaths()).containsValue(
                newArrayList("url1", "url2", "url3")
        );
    }

    /**
     * Assert that any mutating method (defined as any {@link JobConfig} method returning itself, a fluent configuration) throws
     * a {@link IllegalStateException} after {@link JobConfig#lock} has been called
     */
    @Test
    public void when_mutatingLockedJobConfig_then_fail() {
        final JobConfig jobConfig = new JobConfig();

        jobConfig.lock();

        final Stream<Method> mutatorMethods = Arrays.stream(jobConfig.getClass().getMethods())
                .filter(method -> method.getReturnType().equals(jobConfig.getClass()));

        mutatorMethods.forEach(method -> {
            // Set parameters:
            // Reference types to null,
            // Primitives to default
            final Object[] params = Arrays.stream(method.getParameterTypes()).map(Defaults::defaultValue).toArray();

            final Exception thrown = Assert.assertThrows(method.toString(), Exception.class,
                    () -> method.invoke(jobConfig, params));

            assertEquals(method.toString(), IllegalStateException.class, thrown.getCause().getClass());
        });
    }

    private static class ObjectSerializer implements StreamSerializer<Object> {

        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void write(ObjectDataOutput out, Object object) {
        }

        @Override
        public Object read(ObjectDataInput in) {
            return null;
        }
    }
}
