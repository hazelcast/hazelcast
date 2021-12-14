/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

@SuppressWarnings("checkstyle:UnusedImports")
public final class DataSerializerHookScanner {
    private static final String DATA_SERIALIZER_HOOK_PATH_SUFFIX =
            "/src/main/resources/META-INF/services/com.hazelcast.DataSerializerHook";
    private static final String OUTPUT_FILE_SUFFIX = "/hazelcast/src/main/resources/integrity-checker-config.yaml";
    private static final Set<String> EXCLUDED_DIRECTORIES = new HashSet<>(Arrays.asList(
            ".git",
            ".idea",
            ".github",
            ".mvn",
            "docs",
            "checkstyle",
            "images",
            "licenses",
            "target",
            "src"
    ));
    private static final Set<String> VISITED_PATHS = new HashSet<>();
    private static final Map<String, ModuleInfo> FOUND_MODULES = new HashMap<>();

    private DataSerializerHookScanner() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Missing project root dir argument");
        }

        final String rootDir = getRealPath(args[0]);
        final String outputFilePath = getRealPath(rootDir + OUTPUT_FILE_SUFFIX);

        scanForModules(rootDir);

        final BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputFilePath));
        outputWriter.write("# WARNING: This file is regenerated on every build, "
                + "however the generated text is stable between builds.\n");
        outputWriter.write("modules:\n");
        for (final ModuleInfo moduleInfo : FOUND_MODULES.values()) {
            if (moduleInfo.getSerializerHooks().isEmpty()) {
                continue;
            }

            outputWriter.write("  " + moduleInfo.getName() + ":\n");
            for (final String hook : moduleInfo.getSerializerHooks()) {
                outputWriter.write("    - " + hook + "\n");
            }
        }
        outputWriter.flush();
    }

    private static void scanForModules(
            final String currentPath
    ) {
        final File currentDir = new File(currentPath);
        if (!currentDir.isDirectory()) {
            throw new IllegalArgumentException("Path " + currentPath + " is not a directory.");
        }

        if (isModuleDir(currentPath)) {
            tryRegisterModule(currentPath);
        }

        VISITED_PATHS.add(currentPath);

        final File[] fileList = currentDir.listFiles();
        if (fileList == null || fileList.length == 0) {
            return;
        }

        final List<File> dirList = Arrays.stream(fileList)
                .filter(f -> f.isDirectory() && !EXCLUDED_DIRECTORIES.contains(f.getName()))
                .collect(Collectors.toList());

        for (final File file : dirList) {
            if (VISITED_PATHS.contains(getRealPath(file.getAbsolutePath()))) {
                continue;
            }

            if (!isModuleDir(getRealPath(file.getAbsolutePath()))) {
                continue;
            }

            scanForModules(getRealPath(file.getAbsolutePath()));
        }
    }

    private static void tryRegisterModule(final String path) {
        final Model pom = getPom(path);
        if (pom == null) {
            return;
        }

        final String moduleName = pom.getArtifactId();
        FOUND_MODULES.put(moduleName, new ModuleInfo(path, moduleName));

        if (pom.getParent() != null && !FOUND_MODULES.containsKey(pom.getParent().getArtifactId())) {
            return;
        }

        final List<String> hooks = getHooks(path);
        if (hooks.isEmpty()) {
            return;
        }

        FOUND_MODULES.put(moduleName, new ModuleInfo(path, moduleName, hooks));
    }

    private static String getRealPath(String path) {
        try {
            return new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not canonicalize path " + path, e);
        }
    }

    private static Model getPom(final String dir) {
        final File pomFile = new File(dir + "/pom.xml");
        if (!pomFile.exists()) {
            return null;
        }

        final MavenXpp3Reader reader = new MavenXpp3Reader();
        try {
            return reader.read(new FileReader(pomFile));
        } catch (IOException | XmlPullParserException e) {
            return null;
        }
    }

    private static boolean isModuleDir(final String dir) {
        return new File(dir + "/pom.xml").exists();
    }

    private static List<String> getHooks(String sourceModuleDir) {
        try {
            return Files.readAllLines(Paths.get(sourceModuleDir + DATA_SERIALIZER_HOOK_PATH_SUFFIX)).stream()
                    .filter(s -> !s.startsWith("#") && !s.isEmpty())
                    .map(String::trim)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return emptyList();
        }
    }

    private static final class ModuleInfo {
        private String name;
        private String path;
        private List<String> serializerHooks = emptyList();
        private List<ModuleInfo> children = new ArrayList<>();

        ModuleInfo(final String path, final String name) {
            this.path = path;
            this.name = name;
        }

        ModuleInfo(final String path, final String name, final List<String> serializerHooks) {
            this.path = path;
            this.name = name;
            this.serializerHooks = serializerHooks;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getPath() {
            return path;
        }

        public void setPath(final String path) {
            this.path = path;
        }

        public List<String> getSerializerHooks() {
            return serializerHooks;
        }

        public void setSerializerHooks(final List<String> serializerHooks) {
            this.serializerHooks = serializerHooks;
        }

        public List<ModuleInfo> getChildren() {
            return children;
        }

        public void setChildren(final List<ModuleInfo> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "ModuleServicesInfo{"
                    + "name='" + name + '\''
                    + ", path='" + path + '\''
                    + ", serializerHooks=" + serializerHooks
                    + '}';
        }
    }
}
