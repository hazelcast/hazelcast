/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

public final class IOUtil {

    private static final int BUFFER_SIZE = 1 << 14;

    private IOUtil() {
    }

    /**
     * Creates a ZIP-file stream from the directory tree rooted at the supplied
     * {@code baseDir}. Copies the stream into the provided output stream, closing
     * it when done.
     * <p>
     * <strong>Note:</strong> hidden files and directories are ignored
     */
    public static void packDirectoryIntoZip(@Nonnull Path baseDir, @Nonnull OutputStream destination)
            throws IOException {
        try (
                ZipOutputStream zipOut = new ZipOutputStream(destination);
                Stream<Path> fileStream = Files.walk(baseDir)
        ) {
            fileStream.forEach(p -> {
                try {
                    if (Files.isHidden(p) || p == baseDir) {
                        return;
                    }
                    String relativePath = baseDir.relativize(p).toString();
                    boolean directory = Files.isDirectory(p);
                    // slash has been added instead of File.separator since ZipEntry.isDirectory is checking against it.
                    relativePath = directory ? relativePath + "/" : relativePath;
                    zipOut.putNextEntry(new ZipEntry(relativePath));
                    if (!directory) {
                        Files.copy(p, zipOut);
                    }
                    zipOut.closeEntry();
                } catch (IOException e) {
                    throw sneakyThrow(e);
                }
            });
        }
    }

    /**
     * Creates a ZIP-file stream from the supplied input stream. The input will
     * be stored in a single file in the created zip. The {@code destination}
     * stream will be closed.
     *
     * @param source the stream to copy from
     * @param destination the stream to write to
     * @param fileName the name of the file in the destination ZIP
     */
    public static void packStreamIntoZip(
            @Nonnull InputStream source, @Nonnull OutputStream destination, @Nonnull String fileName
    ) throws IOException {
        try (
                ZipOutputStream dstZipStream = new ZipOutputStream(destination)
        ) {
            dstZipStream.putNextEntry(new ZipEntry(fileName));
            copyStream(source, dstZipStream);
        }
    }

    public static void copyStream(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[BUFFER_SIZE];
        for (int readCount; (readCount = in.read(buf)) > 0; ) {
            out.write(buf, 0, readCount);
        }
    }

    public static void unzip(InputStream is, Path targetDir) throws IOException {
        targetDir = targetDir.toAbsolutePath();
        try (ZipInputStream zipIn = new ZipInputStream(is)) {
            for (ZipEntry ze; (ze = zipIn.getNextEntry()) != null; ) {
                Path resolvedPath = targetDir.resolve(ze.getName()).normalize();
                if (!resolvedPath.startsWith(targetDir)) {
                    // see: https://snyk.io/research/zip-slip-vulnerability
                    throw new RuntimeException("Entry with an illegal path: "
                            + ze.getName());
                }
                if (ze.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    Path dir = resolvedPath.getParent();
                    assert dir != null : "null parent: " + resolvedPath;
                    Files.createDirectories(dir);
                    Files.copy(zipIn, resolvedPath);
                }
            }
        }
    }

    /**
     * Extracts the file name from the URL. File name is the part of {@code
     * url.getPath()} after the last '/' character. Returns empty string if the
     * path ends with a '/'.
     * <p>
     * Returns null if input is null or if {@code url.getPath()} returns null.
     */
    @Nullable
    public static String fileNameFromUrl(@Nullable URL url) {
        String fnamePath;
        if (url == null || (fnamePath = url.getPath()) == null) {
            return null;
        }
        // URLs always use forward slash to separate directories
        int lastSlash = fnamePath.lastIndexOf('/');
        return lastSlash < 0 ? fnamePath : fnamePath.substring(lastSlash + 1);
    }


    @Nonnull
    public static String canonicalName(String directory) {
        return Util.uncheckCall(() -> new File(directory).getCanonicalPath());
    }

    /**
     * Resolves the user-provided path against a base directory and validates that the resolved path
     * is contained within that base directory, resolving symbolic links (including links in
     * intermediate directories) to prevent directory traversal.
     * <p>
     * @param baseDir  the trusted base/sandbox directory; must exist
     * @param userPath the user-supplied relative path/filename string
     * @return the resolved and validated absolute Path (may not yet exist)
     * @throws IllegalArgumentException if the path is absolute or rooted, is a symbolic link,
     *                                  resolves outside the base directory, or names the base directory itself
     * @throws IOException              if an I/O error occurs during path resolution
     */
    public static Path resolveAndValidatePath(@Nonnull Path baseDir, @Nonnull String userPath) throws IOException {
        if (!Files.isDirectory(baseDir)) {
            throw new IllegalArgumentException("Base directory does not exist or is not a directory: " + baseDir);
        }
        Path resolvedBase = baseDir.toRealPath();

        Path userPathObj;
        try {
            userPathObj = Paths.get(userPath);
        } catch (java.nio.file.InvalidPathException e) {
            throw new IllegalArgumentException("Invalid path: " + userPath, e);
        }
        // getRoot() != null also catches Windows drive-relative paths like "C:foo",
        // which are not absolute but has a root component
        if (userPathObj.isAbsolute() || userPathObj.getRoot() != null) {
            throw new IllegalArgumentException("Absolute or rooted paths are not allowed: " + userPath);
        }

        Path targetPath = resolvedBase.resolve(userPathObj).normalize();


        // Reject a symlink at the target itself; Files.exists() follows links, so a dangling
        // symlink would otherwise be treated as a non-existent file and returned unresolved
        if (Files.isSymbolicLink(targetPath)) {
            throw new IllegalArgumentException("Symbolic links are not allowed: " + userPath);
        }

        if (Files.exists(targetPath, LinkOption.NOFOLLOW_LINKS)) {
            targetPath = targetPath.toRealPath();
        } else {
            // Walk up to the nearest existing ancestor and resolve its real path, so a
            // symlinked intermediate directory cannot escape the target outside the base
            Path existing = targetPath.getParent();
            Path tail = targetPath.getFileName();
            while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
                tail = existing.getFileName().resolve(tail);
                existing = existing.getParent();
            }
            if (existing == null) {
                throw new IllegalArgumentException("Path cannot be resolved: " + userPath);
            }
            targetPath = existing.toRealPath().resolve(tail);
        }

        if (!targetPath.startsWith(resolvedBase)) {
            throw new IllegalArgumentException("Path traversal attempt detected: " + userPath);
        }

        return targetPath;
    }

    /**
     * Checks whether given name resolves in baseDir. Helps to detect malicious escapes from baseDir.
     * @param baseDir The base directory expected to parent to the files.
     * @param name The file name expected to land in base dir.
     * @return Resolved path in base directory.
     * @throws IOException if name escapes from base directory.
     */
    public static Path resolveInDir(Path baseDir, String name) throws IOException {
        Path base = baseDir.toRealPath();  // which exists
        Path resolved = base.resolve(name).normalize();
        if (!resolved.startsWith(base)) {
            throw new IllegalArgumentException("File name escapes base directory: " + name);
        }
        return resolved;
    }
}

