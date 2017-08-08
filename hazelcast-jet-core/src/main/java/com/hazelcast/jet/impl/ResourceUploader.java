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

final class ResourceUploader {

    private ResourceUploader() { }

    static void uploadMetadata(IMap<String, byte[]> resourcesMap, long jobId, JobConfig jobConfig) {
        Map<String, byte[]> tmpMap = new HashMap<>();

        try {
            for (ResourceConfig rc : jobConfig.getResourceConfigs()) {
                if (rc.isArchive()) {
                    loadJar(tmpMap, jobId, rc.getUrl());
                } else {
                    readStreamAndPutCompressedToMap(tmpMap, jobId, rc.getUrl().openStream(), rc.getId());
                }
            }

            // now upload it all
            resourcesMap.putAll(tmpMap);
        } catch (Throwable e) {
            try {
                resourcesMap.destroy();
            } catch (Throwable ignored) {
                // ignore failure in cleanup in exception handler
            }

            throw rethrow(e);
        }
    }

    /**
     * Unzips the Jar archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(Map, long, InputStream, String)}.
     */
    private static void loadJar(Map<String, byte[]> map, long jobId, URL url) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(url.openStream()))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                readStreamAndPutCompressedToMap(map, jobId, jis, jarEntry.getName());
            }
        }
    }

    private static void readStreamAndPutCompressedToMap(Map<String, byte[]> map, long jobId,
                                                        InputStream in, String resourceId) throws IOException {
        String key = jobId + '/' + resourceId;
        // ignore duplicates: the first resource in first jar takes precedence
        if (map.containsKey(key)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
            IOUtil.drainTo(in, compressor);
        }
        map.put(key, baos.toByteArray());
    }
}
