/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static java.util.Objects.requireNonNull;

public class HzSerializableProcessorSuppliersTest extends SimpleTestInClusterSupport {

    private final DAG dag = new DAG();
    private final URL url = requireNonNull(
            getClass().getResource("DataSerializableSuppliers.jar"));
    private final ClassLoader cl = new URLClassLoader(new URL[] {url});

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_metaSupplier() throws Exception {
        ProcessorMetaSupplier metaSupplier =
                ClassLoaderUtil.newInstance(cl, "com.example.DataSerializableSuppliers$MetaSupplier");
        dag.newVertex("v", metaSupplier);
        submitJob();
    }

    @Test
    public void test_pSupplier() throws Exception {
        ProcessorSupplier pSupplier =
                ClassLoaderUtil.newInstance(cl, "com.example.DataSerializableSuppliers$PSupplier");
        dag.newVertex("v", pSupplier);
        submitJob();
    }

    @Test
    public void test_simpleSupplier() throws Exception {
        SupplierEx<Processor> supplier =
                ClassLoaderUtil.newInstance(cl, "com.example.DataSerializableSuppliers$SimpleSupplier");
        dag.newVertex("v", supplier);
        submitJob();
    }

    private void submitJob() {
        JobConfig config = new JobConfig();
        config.addJar(url);
        instances()[1].newJob(dag, config).join();
    }
}
