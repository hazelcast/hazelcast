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

package com.hazelcast.jet.impl;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class ResourceUploader {

    private final IMap<String, byte[]> targetMap;
    private final Map<String, byte[]> tmpMap = new HashMap<>();

    ResourceUploader(IMap<String, byte[]> targetMap) {
        this.targetMap = targetMap;
    }

    void uploadMetadata(JobConfig jobConfig) {
        if (!targetMap.isEmpty()) {
            throw new IllegalStateException("Map not empty: " + targetMap.getName());
        }

        try {
            for (ResourceConfig rc : jobConfig.getResourceConfigs()) {
                if (rc.isArchive()) {
                    loadJar(rc.getUrl());
                } else {
                    readStreamAndPutCompressedToMap(rc.getUrl().openStream(), rc.getId());
                }
            }

            // now upload it all
            targetMap.putAll(tmpMap);
        } catch (Throwable e) {
            try {
                targetMap.destroy();
            } catch (Throwable ignored) {
                // ignore failure in cleanup in exception handler
            }

            throw rethrow(e);
        }
    }

    /**
     * Unzips the Jar archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(InputStream, String)}.
     */
    private void loadJar(URL url) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(url.openStream()))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                readStreamAndPutCompressedToMap(jis, jarEntry.getName());
            }
        }
    }

    private void readStreamAndPutCompressedToMap(InputStream in, String resourceId) throws IOException {
        String key = JetClassLoader.METADATA_RESOURCES_PREFIX + resourceId;
        // ignore duplicates: the first resource in first jar takes precedence
        if (tmpMap.containsKey(key)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
            IOUtil.drainTo(in, compressor);
        }
        tmpMap.put(key, baos.toByteArray());
    }
}
