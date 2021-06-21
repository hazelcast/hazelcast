/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package childfirstclassloader;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import org.example.jet.impl.deployment.ResourceCollector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

import static java.util.Collections.nCopies;

public class TestProcessorSupplier implements ProcessorSupplier {

    private SupplierEx<SupplierEx<String>> resourceReaderSupplier;
    private SupplierEx<String> resourceReader;

    public TestProcessorSupplier(SupplierEx<SupplierEx<String>> resourceReaderSupplier) {
        this.resourceReaderSupplier = resourceReaderSupplier;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        resourceReader = resourceReaderSupplier.get();
        ResourceCollector.add("ProcessorSupplier init " + resourceReader.get());
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        ResourceCollector.add("ProcessorSupplier get " + resourceReader.get());
        return nCopies(count, new TestProcessor(resourceReaderSupplier));
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        ResourceCollector.add("ProcessorSupplier close " + resourceReader.get());
    }
}
