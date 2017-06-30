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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.nio.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;

public class JetClassLoader extends ClassLoader {

    public static final String METADATA_RESOURCES_PREFIX = "res:";

    private final IMap<String, Object> jobMetadataMap;

    public JetClassLoader(IMap jobMetadataMap) {
        super(JetClassLoader.class.getClassLoader());
        this.jobMetadataMap = jobMetadataMap;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (isEmpty(name)) {
            return null;
        }
        InputStream classBytesStream = resourceStream(name.replace('.', '/') + ".class");
        if (classBytesStream == null) {
            throw new ClassNotFoundException(name + ". Add it using " + JobConfig.class.getSimpleName()
                    + " or start all members with it on classpath");
        }
        byte[] classBytes = uncheckCall(() -> IOUtil.toByteArray(classBytesStream));
        return defineClass(name, classBytes, 0, classBytes.length);
    }

    @Override
    protected URL findResource(String name) {
        if (isEmpty(name)) {
            return null;
        }
        // we distinguish between the case "resource found, but not accessible by URL" and "resource not found"
        if (jobMetadataMap.containsKey(METADATA_RESOURCES_PREFIX + name)) {
            throw new IllegalArgumentException("Resource not accessible by URL: " + name);
        }
        return null;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (isEmpty(name)) {
            return null;
        }
        return resourceStream(name);
    }

    @SuppressWarnings("unchecked")
    private InputStream resourceStream(String name) {
        byte[] classData = (byte[]) jobMetadataMap.get(METADATA_RESOURCES_PREFIX + name);
        if (classData == null) {
            return null;
        }
        return new InflaterInputStream(new ByteArrayInputStream(classData));
    }

    public IMap<String, Object> getJobMetadataMap() {
        return jobMetadataMap;
    }

    private static boolean isEmpty(String className) {
        return className == null || className.isEmpty();
    }
}
