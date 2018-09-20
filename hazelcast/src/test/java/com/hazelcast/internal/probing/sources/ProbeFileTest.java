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

package com.hazelcast.internal.probing.sources;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.probing.AbstractProbeTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeFileTest extends AbstractProbeTest {

    final File userHome = new File(System.getProperty("user.home"));

    @Before
    public void setup() {
        registry.register(MachineProbeSource.INSTANCE);
    }

    @Test
    public void freeSpace() {
        assertProbed("type=file.partition instance=user.home freeSpace", userHome.getFreeSpace(), 0.1);
    }

    @Test
    public void totalSpace() {
        assertProbed("type=file.partition instance=user.home totalSpace", userHome.getTotalSpace(),  0.1);
    }

    @Test
    public void usableSpace() {
        assertProbed("type=file.partition instance=user.home usableSpace", userHome.getUsableSpace(), 0.1);
    }

}
