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
import com.hazelcast.jet.core.AbstractProcessor;
import org.example.jet.impl.deployment.ResourceCollector;

import javax.annotation.Nonnull;

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

}
