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

package com.hazelcast.jet.impl.deployment;

import childfirstclassloader.TestProcessor;
import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import org.example.jet.impl.deployment.ResourceCollector;
import com.hazelcast.jet.impl.processor.NoopP;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

public class ProcessorMetaSupplierUsingClassloader implements ProcessorMetaSupplier {

    SupplierEx<SupplierEx<String>> resourceReaderSupplier;

    public ProcessorMetaSupplierUsingClassloader() {
    }

    public ProcessorMetaSupplierUsingClassloader(SupplierEx<SupplierEx<String>> resourceReaderSupplier) {
        this.resourceReaderSupplier = resourceReaderSupplier;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        resourceReaderSupplier = TestProcessor.ResourceReaderFactory.createResourceReader();
        String resourceInit = "ProcessorMetaSupplier init " + resourceReaderSupplier.get().get();
        ResourceCollector.add(resourceInit);
    }

    @Nonnull @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        String resourceGet = "ProcessorMetaSupplier get " + resourceReaderSupplier.get().get();
        ResourceCollector.add(resourceGet);
        return address -> ProcessorSupplier.of(NoopP::new);
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        String resourceClose = "ProcessorMetaSupplier close " + resourceReaderSupplier.get().get();
        ResourceCollector.add(resourceClose);
    }
}
