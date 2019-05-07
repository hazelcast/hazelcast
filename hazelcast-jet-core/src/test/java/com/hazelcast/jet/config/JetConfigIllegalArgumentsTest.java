/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.jet.config.JetConfig.loadDefault;
import static com.hazelcast.jet.config.JetConfig.loadFromClasspath;
import static com.hazelcast.jet.config.JetConfig.loadFromFile;
import static com.hazelcast.jet.config.JetConfig.loadXmlFromString;
import static com.hazelcast.jet.config.JetConfig.loadYamlFromStream;
import static com.hazelcast.jet.config.JetConfig.loadYamlFromString;
import static java.lang.Thread.currentThread;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class JetConfigIllegalArgumentsTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final SupplierEx<JetConfig> configSupplier;
    private final Class<? extends Throwable> expectedExceptionClass;

    public JetConfigIllegalArgumentsTest(SupplierEx<JetConfig> configSupplier,
                                         Class<? extends Throwable> expectedExceptionClass) {
        this.configSupplier = configSupplier;
        this.expectedExceptionClass = expectedExceptionClass;
    }

    @Parameters(name = "{index}: expected={1}")
    @SuppressWarnings("checkstyle:LineLength")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {(SupplierEx<JetConfig>) () -> loadDefault(null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadXmlFromString(null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadXmlFromString(null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadXmlFromString("aaa"), InvalidConfigurationException.class},
                {(SupplierEx<JetConfig>) () -> loadXmlFromString(null, null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadXmlFromString("aaa", null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromStream(null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromStream(null, null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromString(null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromString("aaa"), InvalidConfigurationException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromString(null, null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromString("aaa", null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromStream(null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadYamlFromStream(null, null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromFile((File) null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromFile(new File("test"), null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromFile(new File("non-existent.xml")), FileNotFoundException.class},
                {(SupplierEx<JetConfig>) () -> loadFromFile(new File("non-existent.yml")), FileNotFoundException.class},
                {(SupplierEx<JetConfig>) () -> loadFromClasspath(currentThread().getContextClassLoader(), "non-existent.xml"), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromClasspath(currentThread().getContextClassLoader(), "non-existent.yml"), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromClasspath(currentThread().getContextClassLoader(), null, System.getProperties()), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromClasspath(currentThread().getContextClassLoader(), "test", null), IllegalArgumentException.class},
                {(SupplierEx<JetConfig>) () -> loadFromClasspath(null, "test"), IllegalArgumentException.class},
        });
    }

    @Test
    public void when_loadedWithMissingOrWrongParameters_thenThrowsException() {
        exception.expect(expectedExceptionClass);
        configSupplier.get();
    }

}
