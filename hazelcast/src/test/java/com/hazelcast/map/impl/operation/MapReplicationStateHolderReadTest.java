/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE;
import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.cluster.Versions.V3_9;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapReplicationStateHolderReadTest {

    @Parameters(name = "v:{0}, ee:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                // version, isEnterprise, shouldRead
                {V3_9, false, true},
                {V3_9, true, false},
                {V3_10, false, true},
                {V3_10, true, true},
        });
    }

    @Parameter
    public Version streamVersion;

    @Parameter(1)
    public boolean enterprise;

    @Parameter(2)
    public boolean shouldReadIndexInfo;

    private ObjectDataInput input;
    private MapReplicationStateHolder holder;

    @Before
    public void setup() throws IOException {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE, Boolean.toString(enterprise));
        holder = new MapReplicationStateHolder();

        input = Mockito.mock(ObjectDataInput.class);
        when(input.getVersion()).thenReturn(streamVersion);
        when(input.readInt()).thenReturn(0, 0, 1);
    }

    @After
    public void tearDown() {
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE);
    }

    @Test
    public void test_readsMapIndexInfo() throws IOException {
        holder.readData(input);
        InOrder inOrder = inOrder(input);
        inOrder.verify(input, times(2)).readInt();
        inOrder.verify(input).getVersion();
        if (shouldReadIndexInfo) {
            inOrder.verify(input).readInt();
            inOrder.verify(input).readObject();
        }
        inOrder.verifyNoMoreInteractions();
    }

}
