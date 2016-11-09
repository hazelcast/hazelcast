/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.jet2.impl.deployment;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class DeploymentStore {

    private static final ILogger LOGGER = Logger.getLogger(DeploymentStore.class);
    private static final int KILOBYTE = 1024;

    private final String storagePath;
    private final File storageDirectory;
    private final int chunkSize;

    private final Map<DeploymentDescriptor, File> resources = new ConcurrentHashMap<>();
    private final Map<String, ClassLoaderEntry> jarEntries = new HashMap<>();
    private final Map<String, ClassLoaderEntry> dataEntries = new HashMap<>();
    private final Map<String, ClassLoaderEntry> classEntries = new HashMap<>();

    private long fileNameCounter = 1;

    public DeploymentStore(String storagePath, int chunkSize) {
        this.storagePath = createTempIfNull(storagePath);
        this.chunkSize = chunkSize;
        this.storageDirectory = createStorageDirectory();
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

    private File createStorageDirectory() {
        Path storageDirectoryPath = Paths.get(storagePath);
        int directoryNameCounter = 0;
        do {
            try {
                storageDirectoryPath = Files.createDirectory(storageDirectoryPath);
            } catch (FileAlreadyExistsException e) {
                storageDirectoryPath = Paths.get(storagePath + File.pathSeparator + '_' + directoryNameCounter++);
            } catch (IOException e) {
                throw rethrow(e);
            }

        } while (!storageDirectoryPath.toFile().exists());
        return storageDirectoryPath.toFile();
    }

    public void destroy() {
        IOUtil.delete(storageDirectory);
    }

    public int getChunkSize() {
        return chunkSize;
    }

    private File createResource(DeploymentDescriptor descriptor) {
        String path = generateResourcePath();
        File file = new File(path);
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new Error("Deployment failure, unable to create a file -> " + path);
                }
            } catch (IOException e) {
                throw new Error("Deployment failure, unable to create a file -> " + path);
            }
        }
        if (!file.canWrite()) {
            throw new Error("Unable to write to the file " + path + " - file is not permitted to write");
        }
        resources.put(descriptor, file);
        return file;
    }

    synchronized void receiveChunk(ResourceChunk chunk) {
        DeploymentDescriptor descriptor = chunk.getDescriptor();
        if (!resources.containsKey(descriptor)) {
            createResource(descriptor);
        }
        File file = resources.get(descriptor);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(chunk.getSequence() * chunkSize);
            raf.write(chunk.getBytes());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private String generateResourcePath() {
        return storageDirectory + File.pathSeparator + "resource" + fileNameCounter++;
    }

    private static String createTempIfNull(String storagePath) {
        if (storagePath != null) {
            return storagePath;
        }
        try {
            return Files.createTempDirectory("hazelcast-jet-").toString();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    void updateCatalog(DeploymentDescriptor descriptor) {
        try (ResourceStream resourceStream = openResource(resources.get(descriptor))) {
            switch (descriptor.getDeploymentType()) {
                case JAR:
                    loadJarStream(resourceStream);
                    return;
                case CLASS:
                    loadClassStream(descriptor, resourceStream);
                    return;
                case DATA:
                    loadDataStream(descriptor, resourceStream);
                    return;
                default:
                    throw new AssertionError("Unhandled deployment type " + descriptor.getDeploymentType());
            }
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private void loadClassStream(DeploymentDescriptor descriptor, ResourceStream resourceStream) {
        byte[] classBytes = readFully(resourceStream.inputStream);
        classEntries.put(descriptor.getId(), new ClassLoaderEntry(classBytes, resourceStream.baseUrl));
    }

    private void loadDataStream(DeploymentDescriptor descriptor, ResourceStream resourceStream) {
        byte[] bytes = readFully(resourceStream.inputStream);
        dataEntries.put(descriptor.getId(), new ClassLoaderEntry(bytes, resourceStream.baseUrl));
    }

    private void loadJarStream(ResourceStream resourceStream) throws IOException {
        BufferedInputStream bis = null;
        JarInputStream jis = null;

        try {
            bis = new BufferedInputStream(resourceStream.inputStream);
            jis = new JarInputStream(bis);
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buf = new byte[KILOBYTE];
                for (int len; (len = jis.read(buf)) > 0; ) {
                    out.write(buf, 0, len);
                }
                String name = jarEntry.getName();
                String clazzSuffix = ".class";
                if (jarEntry.getName().endsWith(clazzSuffix)) {
                    name = name.substring(0, name.length() - clazzSuffix.length()).replace("/", ".");
                }
                ClassLoaderEntry entry = new ClassLoaderEntry(out.toByteArray(),
                        String.format("jar:%s!/%s", resourceStream.baseUrl, name));
                jarEntries.put(name, entry);
            }
        } finally {
            close(bis, jis);
        }
    }

    private static ResourceStream openResource(File resource) throws IOException {
        try {
            return new ResourceStream(new FileInputStream(resource), resource.toURI().toURL().toString());
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static byte[] readFully(InputStream in) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] b = new byte[KILOBYTE];
            for (int len; (len = in.read(b)) != -1; ) {
                out.write(b, 0, len);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    private static void close(Closeable... closeables) {
        if (closeables == null) {
            return;
        }
        Throwable error = null;
        for (Closeable closeable : closeables) {
            if (closeable == null) {
                continue;
            }
            try {
                closeable.close();
            } catch (IOException e) {
                error = e;
            }
        }
        if (error != null) {
            throw rethrow(error);
        }
    }

    private static class ResourceStream implements Closeable {
        final String baseUrl;
        final InputStream inputStream;

        ResourceStream(InputStream inputStream, String baseUrl) {
            this.inputStream = inputStream;
            this.baseUrl = baseUrl;
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }
    }
}
