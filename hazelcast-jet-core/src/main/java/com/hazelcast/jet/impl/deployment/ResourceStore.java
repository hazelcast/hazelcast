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

import com.hazelcast.nio.IOUtil;
import com.hazelcast.util.UuidUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.read;

public class ResourceStore {

    private static final int BUFFER_SIZE = 1024;

    private final Path storageDirectory;

    private final Map<ResourceDescriptor, File> resources = new ConcurrentHashMap<>();
    private final Map<String, ClassLoaderEntry> jarEntries = new ConcurrentHashMap<>();
    private final Map<String, ClassLoaderEntry> dataEntries = new ConcurrentHashMap<>();
    private final Map<String, ClassLoaderEntry> classEntries = new ConcurrentHashMap<>();

    public ResourceStore(String storagePath) {
        this.storageDirectory = createStorageDirectory(storagePath);
    }

    public void destroy() {
        IOUtil.delete(storageDirectory.toFile());
    }

    Map<String, ClassLoaderEntry> getJarEntries() {
        return jarEntries;
    }

    Map<String, ClassLoaderEntry> getDataEntries() {
        return dataEntries;
    }

    Map<String, ClassLoaderEntry> getClassEntries() {
        return classEntries;
    }

    synchronized void updateResource(ResourcePart part) throws IOException {
        File file = resources.computeIfAbsent(part.getDescriptor(), this::createResource);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(part.getOffset());
            raf.write(part.getBytes());
        }
    }

    void completeResource(ResourceDescriptor descriptor) throws IOException {
        File resource = resources.get(descriptor);
        try (FileInputStream stream = new FileInputStream(resource)) {
            String resourceUri = resource.toURI().toString();
            switch (descriptor.getResourceKind()) {
                case JAR:
                    loadJarStream(stream, resourceUri);
                    return;
                case CLASS:
                    classEntries.put(descriptor.getId(), new ClassLoaderEntry(read(stream), resourceUri));
                    return;
                case DATA:
                    dataEntries.put(descriptor.getId(), new ClassLoaderEntry(read(stream), resourceUri));
                    return;
                default:
                    throw new AssertionError("Unhandled resource type " + descriptor.getResourceKind());
            }
        }
    }

    private File createResource(ResourceDescriptor descriptor) {
        String fileName = descriptor.getId() + '-' + UuidUtil.newUnsecureUuidString();
        File file = Paths.get(storageDirectory.toString(), fileName).toFile();
        try {
            if (!file.createNewFile()) {
                throw new IOException("File " + file + " already exists.");
            }
            return file;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private void loadJarStream(FileInputStream stream, String uri) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(stream))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buf = new byte[BUFFER_SIZE];
                for (int len; (len = jis.read(buf)) > 0; ) {
                    out.write(buf, 0, len);
                }
                String name = jarEntry.getName();
                String clazzSuffix = ".class";
                if (jarEntry.getName().endsWith(clazzSuffix)) {
                    name = name.substring(0, name.length() - clazzSuffix.length()).replace("/", ".");
                }
                ClassLoaderEntry entry = new ClassLoaderEntry(out.toByteArray(),
                        String.format("jar:%s!/%s", uri, name));
                jarEntries.put(name, entry);
            }
        }
    }

    private static Path createStorageDirectory(String storagePath) {
        try {
            Path path = Paths.get(storagePath, "resources");
            if (!path.toFile().mkdirs() && !path.toFile().exists()) {
                throw new IOException("Could not create requested storage path " + path);
            }
            return path;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }
}
