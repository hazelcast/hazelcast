/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jca;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ObjectEqualsHashcodeTests {

    @Test
    public void testResourcAdapterImpl() {
        EqualsVerifier.forClass(ResourceAdapterImpl.class).usingGetClass()
                //using non-final fields and transient fields in equals should probably get FIXed
                .suppress(Warning.NONFINAL_FIELDS).suppress(Warning.TRANSIENT_FIELDS).verify();
    }

    @Test
    public void testConnectionFactoryImpl() {
        EqualsVerifier.forClass(ConnectionFactoryImpl.class).usingGetClass().withPrefabValues(ManagedConnectionFactoryImpl.class,
                new ManagedConnectionFactoryImpl(), new ManagedConnectionFactoryImpl())
                //using non-final fields and transient fields in equals should probably get FIXed
                .suppress(Warning.NONFINAL_FIELDS).suppress(Warning.TRANSIENT_FIELDS).verify();
    }

    @Test
    public void testManagedConnectionFactoryImpl() {
        EqualsVerifier.forClass(ManagedConnectionFactoryImpl.class).usingGetClass()
                //library fails to create PrintWriter instance in JcaBase class, which is used for logging.
                //we can safely mock it as it does not affect equality or hashcode
                .withPrefabValues(PrintWriter.class, mock(PrintWriter.class), mock(PrintWriter.class))
                //using non-final fields and transient fields in equals should probably get FIXed
                .suppress(Warning.NONFINAL_FIELDS).suppress(Warning.TRANSIENT_FIELDS).verify();
    }
}
