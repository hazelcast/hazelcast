/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;
import org.codehaus.plexus.util.IOUtil;
import org.codehaus.plexus.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * This transformer implementation is used to merge MANIFEST and OSGi
 * bundle metadata in conjunction with the Maven Shade plugin when
 * integrating multiple dependencies into one output JAR
 */
public class HazelcastManifestTransformer
        implements ResourceTransformer {

    private static final String IMPORT_PACKAGE = "Import-Package";
    private static final String EXPORT_PACKAGE = "Export-Package";

    private final Set<String> importedPackages = new HashSet<String>();
    private final Set<String> exportedPackages = new HashSet<String>();

    private Manifest shadedManifest;

    // Configuration
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_UNWRITTEN_FIELD",
            justification = "Filled by Maven")
    private Map<String, Attributes> manifestEntries;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_UNWRITTEN_FIELD",
            justification = "Filled by Maven")
    private String mainClass;

    @Override
    public boolean canTransformResource(String resource) {
        return resource.equals(JarFile.MANIFEST_NAME);
    }

    @Override
    public void processResource(String resource, InputStream inputStream, List<Relocator> relocators)
            throws IOException {

        Attributes attributes;
        if (shadedManifest == null) {
            shadedManifest = new Manifest(inputStream);
            attributes = shadedManifest.getMainAttributes();
        } else {
            Manifest manifest = new Manifest(inputStream);
            attributes = manifest.getMainAttributes();
        }

        Set<String> imports = new LinkedHashSet<String>();
        Set<String> exports = new LinkedHashSet<String>();

        String importPackages = attributes.getValue(IMPORT_PACKAGE);
        if (importPackages != null) {
            imports.addAll(ElementParser.parseDelimitedString(importPackages, ',', true));
        }

        String exportPackages = attributes.getValue(EXPORT_PACKAGE);
        if (exportPackages != null) {
            exports.addAll(ElementParser.parseDelimitedString(exportPackages, ',', true));
        }

        importedPackages.removeAll(exports);
        importedPackages.addAll(imports);
        exportedPackages.addAll(exports);

        IOUtil.close(inputStream);
    }

    @Override
    public boolean hasTransformedResource() {
        return true;
    }

    @Override
    public void modifyOutputStream(JarOutputStream jarOutputStream)
            throws IOException {

        if (shadedManifest == null) {
            shadedManifest = new Manifest();
        }

        Attributes attributes = shadedManifest.getMainAttributes();
        attributes.putValue(IMPORT_PACKAGE, StringUtils.join(importedPackages.iterator(), ","));
        attributes.putValue(EXPORT_PACKAGE, StringUtils.join(exportedPackages.iterator(), ","));

        if (mainClass != null) {
            attributes.put(Attributes.Name.MAIN_CLASS, mainClass);
        }

        if (manifestEntries != null) {
            for (Map.Entry<String, Attributes> entry : manifestEntries.entrySet()) {
                attributes.put(new Attributes.Name(entry.getKey()), entry.getValue());
            }
        }

        jarOutputStream.putNextEntry(new JarEntry(JarFile.MANIFEST_NAME));
        shadedManifest.write(jarOutputStream);
    }
}
