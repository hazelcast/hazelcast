/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.Address;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;

public class LoadPersonIsolated extends AbstractProcessor {

    @Override
    public boolean complete() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            cl.loadClass("com.sample.pojo.person.Person$Appereance");
        } catch (ClassNotFoundException e) {
            fail(e.getMessage());
        }
        try {
            cl.loadClass("com.sample.pojo.car.Car");
            fail();
        } catch (ClassNotFoundException ignored) {
        }
        return true;
    }

    static class LoadPersonIsolatedSupplier implements ProcessorSupplier {

        private static final long serialVersionUID = 9124364032142382663L;

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(LoadPersonIsolated::new).limit(count).collect(toList());
        }
    }

    static class LoadPersonIsolatedMetaSupplier implements ProcessorMetaSupplier {

        private static final long serialVersionUID = -2678527620814378262L;

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return (Address x) -> new LoadPersonIsolatedSupplier();
        }
    }
}
