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

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LoadResource extends AbstractProcessor {

    private static final String EXPECTED = "AAAP|Advanced Accelerator Applications S.A. - " +
            "American Depositary Shares|Q|N|N|100|N|N";

    @Override
    public boolean complete() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Enumeration<URL> resources = cl.getResources("customId");
            URL resource = resources.nextElement();
            assertNotNull("resource shouldn't be null", resource);
            InputStream resourceAsStream = cl.getResourceAsStream("customId");
            assertNotNull("resource stream shouldn't be null", resourceAsStream);
            readFromStreamAndAssert(resource.openStream());
            readFromStreamAndAssert(resourceAsStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public void readFromStreamAndAssert(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        assertEquals(EXPECTED, reader.readLine());
    }

    static class LoadResourceSupplier implements ProcessorSupplier {

        private static final long serialVersionUID = 9124364422142382663L;

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(LoadResource::new).limit(count).collect(toList());
        }
    }

    static class LoadResourceMetaSupplier implements ProcessorMetaSupplier {

        private static final long serialVersionUID = -2678517620814378262L;

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return (Address x) -> new LoadResourceSupplier();
        }
    }
}
