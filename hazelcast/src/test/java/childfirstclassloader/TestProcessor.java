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

package childfirstclassloader;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import org.apache.commons.io.IOUtils;
import org.example.jet.impl.deployment.ResourceCollector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;

public class TestProcessor extends AbstractProcessor {

    // The supplier is not on test nor member classpath - it is on processor classpath
    private SupplierEx<SupplierEx<String>> resourceReaderSupplier;
    private SupplierEx<String> resourceReader;


    public TestProcessor(SupplierEx<SupplierEx<String>> resourceReaderSupplier) {
        this.resourceReaderSupplier = resourceReaderSupplier;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        resourceReader = resourceReaderSupplier.get();
        ResourceCollector.add("Processor init " + resourceReader.get());
    }


    @Override
    public boolean complete() {
        ResourceCollector.add("Processor complete " + resourceReader.get());
        return tryEmit(resourceReader.get());
    }

    /**
     * Reads contents of `childfirstclassloader/resource_test.txt` resource
     *
     * Depending on whether this class is loaded by system classloader or the processor classloader it will return
     * different content:
     * - system classloader returns content of `target/test-classes/childfirstclassloader/resource_test.txt` file
     * - processor classloader returns content of `childfirstclassloader/resource_test.txt` file inside resources.jar
     */
    public static class ResourceReader implements SupplierEx<String> {

        @Override
        public String getEx() throws Exception {
            InputStream is = ResourceReader.class.getClassLoader().getResourceAsStream("childfirstclassloader/resource_test.txt");
            return IOUtils.toString(is, UTF_8);
        }
    }

    public static class ResourceReaderFactory {

        public static SupplierEx<SupplierEx<String>> createResourceReader() {
            return () -> new ResourceReader();
        }
    }

    public static class TestProcessorSupplier implements ProcessorSupplier {

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

    public static class TestProcessorMetaSupplier implements ProcessorMetaSupplier {

        private SupplierEx<SupplierEx<String>> resourceReaderSupplier;
        private SupplierEx<String> resourceReader;

        public TestProcessorMetaSupplier(SupplierEx<SupplierEx<String>> resourceReaderSupplier) {
            this.resourceReaderSupplier = resourceReaderSupplier;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            resourceReader = resourceReaderSupplier.get();
            ResourceCollector.add("ProcessorMetaSupplier init " + resourceReader.get());
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            ResourceCollector.add("ProcessorMetaSupplier get " + resourceReader.get());
            return address -> {
                ResourceCollector.add("ProcessorMetaSupplier create " + resourceReader.get());
                return new TestProcessorSupplier(resourceReaderSupplier);
            };
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            ResourceCollector.add("ProcessorMetaSupplier close " + resourceReader.get());
        }

        public static TestProcessorMetaSupplier create() {
            return new TestProcessorMetaSupplier(
                    () -> new ResourceReader()
            );
        }
    }
}
