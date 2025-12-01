/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.impl.connector.UpdateMapWithEntryProcessorP.NamespaceAwareEntryProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UpdateMapWithEntryProcessorPTest {

    @Rule
    public TemporaryFolder dir = new TemporaryFolder();

    @Test
    public void testJavaDeserializationDisabledForNamespaceAwareEntryProcessor()
            throws IOException {
        NamespaceAwareEntryProcessor<String, String, String> underTest = new NamespaceAwareEntryProcessor<>(entry -> "",
                "exampleNamespace");

        File f = dir.newFile();
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f))) {
            oos.writeObject(underTest);
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
            assertThatThrownBy(ois::readObject).isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
