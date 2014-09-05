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

import aQute.lib.osgi.Instruction;
import edu.emory.mathcs.backport.java.util.Arrays;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ManifestResourceTransformer;
import org.codehaus.plexus.util.IOUtil;
import org.codehaus.plexus.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
        extends ManifestResourceTransformer {

    private static final String VERSION_PREFIX = "version=";
    private static final String RESOLUTION_PREFIX = "resolution:=";
    private static final String USES_PREFIX = "uses:=";

    private static final int VERSION_OFFSET = 8;
    private static final int USES_OFFSET = 7;

    private static final String IMPORT_PACKAGE = "Import-Package";
    private static final String EXPORT_PACKAGE = "Export-Package";

    private final Map<String, PackageDefinition> importedPackages = new HashMap<String, PackageDefinition>();
    private final Map<String, PackageDefinition> exportedPackages = new HashMap<String, PackageDefinition>();
    private final List<InstructionDefinition> importOverrideInstructions = new ArrayList<InstructionDefinition>();
    private final List<InstructionDefinition> exportOverrideInstructions = new ArrayList<InstructionDefinition>();

    private Manifest shadedManifest;

    // Configuration
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_UNWRITTEN_FIELD",
            justification = "Filled by Maven")
    private Map<String, Attributes> manifestEntries;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_UNWRITTEN_FIELD",
            justification = "Filled by Maven")
    private String mainClass;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_UNWRITTEN_FIELD",
            justification = "Filled by Maven")
    private Map<String, String> overrideInstructions;

    @Override
    public boolean canTransformResource(String resource) {
        return JarFile.MANIFEST_NAME.equalsIgnoreCase(resource);
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

        String importPackages = attributes.getValue(IMPORT_PACKAGE);
        if (importPackages != null) {
            List<String> definitions = ElementParser.parseDelimitedString(importPackages, ',', true);
            for (String definition : definitions) {
                PackageDefinition packageDefinition = new PackageDefinition(definition);

                String packageName = packageDefinition.packageName;
                PackageDefinition oldPackageDefinition = importedPackages.get(packageName);
                importedPackages.put(packageName, findStrongerDefinition(packageDefinition, oldPackageDefinition));
            }
        }

        String exportPackages = attributes.getValue(EXPORT_PACKAGE);
        if (exportPackages != null) {
            List<String> definitions = ElementParser.parseDelimitedString(exportPackages, ',', true);
            for (String definition : definitions) {
                PackageDefinition packageDefinition = new PackageDefinition(definition);

                String packageName = packageDefinition.packageName;
                PackageDefinition oldPackageDefinition = exportedPackages.get(packageName);
                exportedPackages.put(packageName, mergeExportUsesConstraint(packageDefinition, oldPackageDefinition));
            }
        }

        IOUtil.close(inputStream);
    }

    private PackageDefinition findStrongerDefinition(PackageDefinition packageDefinition,
                                                     PackageDefinition oldPackageDefinition) {

        // If no old definition or new definition is required import we take the new one
        if (oldPackageDefinition == null
                || oldPackageDefinition.resolutionOptional && !packageDefinition.resolutionOptional) {
            return packageDefinition;
        }

        // If old definition was required import but new isn't we take the old one
        if (!oldPackageDefinition.resolutionOptional && packageDefinition.resolutionOptional) {
            return oldPackageDefinition;
        }

        if (oldPackageDefinition.version == null && packageDefinition.version != null) {
            return packageDefinition;
        }

        if (oldPackageDefinition.version != null && packageDefinition.version == null) {
            return oldPackageDefinition;
        }

        return oldPackageDefinition;
    }

    private PackageDefinition mergeExportUsesConstraint(PackageDefinition packageDefinition,
                                                        PackageDefinition oldPackageDefinition) {

        Set<String> uses = new LinkedHashSet<String>();
        if (oldPackageDefinition != null) {
            uses.addAll(oldPackageDefinition.uses);
        }
        uses.addAll(packageDefinition.uses);

        String packageName = packageDefinition.packageName;
        boolean resolutionOptional = packageDefinition.resolutionOptional;
        String version = packageDefinition.version;
        return new PackageDefinition(packageName, resolutionOptional, version, uses);
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

        precompileOverrideInstructions();

        Attributes attributes = shadedManifest.getMainAttributes();
        attributes.putValue(IMPORT_PACKAGE, StringUtils.join(shadeImports().iterator(), ","));
        attributes.putValue(EXPORT_PACKAGE, StringUtils.join(shadeExports().iterator(), ","));

        attributes.putValue("Created-By", "HazelcastManifestTransformer through Shade Plugin");

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
        jarOutputStream.flush();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_UNWRITTEN_FIELD",
            justification = "Field is set by Maven")
    private void precompileOverrideInstructions() {
        String importPackageInstructions = overrideInstructions.get(IMPORT_PACKAGE);
        if (importPackageInstructions != null) {
            List<String> packageInstructions = ElementParser.parseDelimitedString(importPackageInstructions, ',', true);
            for (String packageInstruction : packageInstructions) {
                PackageDefinition packageDefinition = new PackageDefinition(packageInstruction);
                Instruction instruction = Instruction.getPattern(packageDefinition.packageName);
                System.out.println("Compiled import instruction '" + packageInstruction + "' -> " + instruction);
                importOverrideInstructions.add(new InstructionDefinition(packageDefinition, instruction));
            }
        }
        String exportPackageInstructions = overrideInstructions.get(EXPORT_PACKAGE);
        if (exportPackageInstructions != null) {
            List<String> packageInstructions = ElementParser.parseDelimitedString(exportPackageInstructions, ',', true);
            for (String packageInstruction : packageInstructions) {
                PackageDefinition packageDefinition = new PackageDefinition(packageInstruction);
                Instruction instruction = Instruction.getPattern(packageDefinition.packageName);
                System.out.println("Compiled export instruction '" + packageInstruction + "' -> " + instruction);
                exportOverrideInstructions.add(new InstructionDefinition(packageDefinition, instruction));
            }
        }
    }

    private Set<String> shadeExports() {
        Set<String> exports = new LinkedHashSet<String>();
        for (Map.Entry<String, PackageDefinition> entry : exportedPackages.entrySet()) {
            String definition = entry.getValue().buildDefinition(false);
            exports.add(definition);
            System.out.println("Adding shaded export -> " + definition);
        }
        return exports;
    }

    private Set<String> shadeImports() {
        for (String export : exportedPackages.keySet()) {
            PackageDefinition definition = new PackageDefinition(export);
            importedPackages.remove(definition.packageName);
        }
        Set<String> imports = new LinkedHashSet<String>();
        for (Map.Entry<String, PackageDefinition> entry : importedPackages.entrySet()) {
            PackageDefinition original = entry.getValue();
            PackageDefinition overridden = overridePackageDefinitionResolution(original);
            String definition = overridden.buildDefinition(true);
            imports.add(definition);
            System.out.println("Adding shaded import -> " + definition);
        }
        return imports;
    }

    private PackageDefinition overridePackageDefinitionResolution(PackageDefinition packageDefinition) {
        for (InstructionDefinition instructionDefinition : importOverrideInstructions) {
            Instruction instruction = instructionDefinition.instruction;
            boolean instructed = !instruction.isNegated() == instruction.matches(packageDefinition.packageName);
            if (instructed) {
                System.out.println("Instruction '" + instruction + "' -> package '" + packageDefinition.packageName + "'");

                PackageDefinition override = instructionDefinition.packageDefinition;
                String packageName = packageDefinition.packageName;
                String version = packageDefinition.version;
                Set<String> uses = packageDefinition.uses;
                return new PackageDefinition(packageName, override.resolutionOptional, version, uses);
            }
        }

        return packageDefinition;
    }

    private static final class PackageDefinition {
        private final String packageName;
        private final boolean resolutionOptional;
        private final String version;
        private final Set<String> uses;

        private PackageDefinition(String definition) {
            String[] tokens = definition.split(";");
            this.packageName = tokens[0];
            this.resolutionOptional = findResolutionConstraint(tokens);
            this.version = findVersionConstraint(tokens);
            this.uses = findUsesConstraint(tokens);
        }

        private PackageDefinition(String packageName, boolean resolutionOptional, String version, Set<String> uses) {
            this.packageName = packageName;
            this.resolutionOptional = resolutionOptional;
            this.version = version;
            this.uses = new LinkedHashSet<String>(uses);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PackageDefinition that = (PackageDefinition) o;

            if (packageName != null ? !packageName.equals(that.packageName) : that.packageName != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return packageName != null ? packageName.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "PackageDefinition{" + "packageName='" + packageName + '\'' + ", resolutionOptional=" + resolutionOptional
                    + ", version='" + version + '\'' + ", uses=" + uses + '}';
        }

        public String buildDefinition(boolean addResolutionConstraint) {
            StringBuilder sb = new StringBuilder(packageName);
            if (addResolutionConstraint && resolutionOptional) {
                sb.append(";").append(RESOLUTION_PREFIX).append("optional");
            }
            if (version != null) {
                sb.append(";").append(VERSION_PREFIX).append(version);
            }
            if (uses != null && !uses.isEmpty()) {
                sb.append(";").append(USES_PREFIX).append('"').append(StringUtils.join(uses.iterator(), ",")).append('"');
            }
            return sb.toString();
        }

        private String findVersionConstraint(String[] tokens) {
            for (String token : tokens) {
                if (token.startsWith(VERSION_PREFIX)) {
                    return token.substring(VERSION_OFFSET);
                }
            }
            return null;
        }

        private boolean findResolutionConstraint(String[] tokens) {
            for (String token : tokens) {
                if (token.startsWith(RESOLUTION_PREFIX)) {
                    return token.equalsIgnoreCase("resolution:=optional");
                }
            }
            return false;
        }

        private Set<String> findUsesConstraint(String[] tokens) {
            for (String token : tokens) {
                if (token.startsWith(USES_PREFIX)) {
                    String packages = token.substring(USES_OFFSET, token.length() - 1);
                    String[] sepPackages = packages.split(",");
                    return new LinkedHashSet<String>(Arrays.asList(sepPackages));
                }
            }
            return Collections.emptySet();
        }
    }

    private static final class InstructionDefinition {
        private final PackageDefinition packageDefinition;
        private final Instruction instruction;

        private InstructionDefinition(PackageDefinition packageDefinition, Instruction instruction) {
            this.packageDefinition = packageDefinition;
            this.instruction = instruction;
        }

        @Override
        public String toString() {
            return "InstructionDefinition{" + "packageDefinition=" + packageDefinition + ", instruction=" + instruction + '}';
        }
    }
}
