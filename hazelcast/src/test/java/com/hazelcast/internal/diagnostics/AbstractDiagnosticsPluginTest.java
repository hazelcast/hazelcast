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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Field;

import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;

public class AbstractDiagnosticsPluginTest extends HazelcastTestSupport {

    protected DiagnosticsLogWriterImpl logWriter;
    private CharArrayWriter out;

    @Before
    public final void setupLogWriter() {
        logWriter = new DiagnosticsLogWriterImpl();
        out = new CharArrayWriter();
        logWriter.init(new PrintWriter(out));
    }

    protected void reset() {
        out.reset();
    }

    protected String getContent() {
        return out.toString();
    }

    protected void assertContains(String expected) {
        assertContains(getContent(), expected);
    }

    protected void assertNotContains(String expected) {
        assertNotContains(getContent(), expected);
    }

    static Diagnostics getDiagnostics(HazelcastInstance hazelcastInstance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hazelcastInstance);
        try {
            Field field = NodeEngineImpl.class.getDeclaredField("diagnostics");
            field.setAccessible(true);
            return (Diagnostics) field.get(nodeEngine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void cleanupDiagnosticFiles(Diagnostics diagnostics) {
        if (diagnostics == null) {
            return;
        }
        File[] files = diagnostics.directory.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String name = file.getName();
            if (name.startsWith(diagnostics.baseFileName) && name.endsWith(".log")) {
                deleteQuietly(file);
            }
        }
    }
}
