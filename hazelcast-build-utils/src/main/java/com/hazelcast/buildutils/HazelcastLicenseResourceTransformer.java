/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.buildutils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ReproducibleResourceTransformer;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;

/**
 * Prevents duplicate copies of the Apache License.
 */
public class HazelcastLicenseResourceTransformer implements ReproducibleResourceTransformer {
    private static final String LICENSE_PATH = "META-INF/LICENSE";

    private static final String LICENSE_TXT_PATH = "META-INF/LICENSE.txt";

    private long time = Long.MIN_VALUE;

    @Override
    public boolean canTransformResource(String resource) {
        return equalsIgnoreCase(resource, LICENSE_PATH) || equalsIgnoreCase(resource, LICENSE_TXT_PATH);
    }

    @Override
    public void processResource(String resource, InputStream is, List<Relocator> relocators, long time) throws IOException {
        if (time > this.time) {
            this.time = time;
        }
    }

    @Override
    public boolean hasTransformedResource() {
        return true;
    }

    @Override
    public void modifyOutputStream(JarOutputStream jos) throws IOException {
        JarEntry jarEntry = new JarEntry(LICENSE_PATH);
        jarEntry.setTime(time);
        jos.putNextEntry(jarEntry);
        jos.write(LicenseHolder.LICENSE);
        jos.closeEntry();
    }

    @Override
    public void processResource(String resource, InputStream is, List<Relocator> relocators) throws IOException {
        processResource(resource, is, relocators, 0);
    }

    @SuppressWarnings({"checkstyle:MagicNumber"})
    static class LicenseHolder {
        public static final byte[] LICENSE;

        static {
            byte[] loaded = null;
            try (InputStream input = LicenseHolder.class.getResourceAsStream("/LICENSE");
                    ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                int n;
                while (-1 != (n = input.read(buffer))) {
                    output.write(buffer, 0, n);
                }
                loaded = output.toByteArray();
            } catch (IOException e) {
                new RuntimeException("Unable to load the LICENSE file", e);
            }
            LICENSE = loaded;
        }
    }
}
