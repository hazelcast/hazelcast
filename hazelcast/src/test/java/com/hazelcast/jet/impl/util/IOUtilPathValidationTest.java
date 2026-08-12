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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.hazelcast.jet.impl.util.IOUtil.resolveAndValidatePath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
class IOUtilPathValidationTest {

    @TempDir
    Path sandbox;

    @TempDir
    Path outside;

    Path base;

    @BeforeEach
    void setUp() throws IOException {
        base = Files.createDirectory(sandbox.resolve("base"));
    }

    // ---------- happy paths ----------

    @Test
    void simpleFilename_isResolvedInsideBase() throws IOException {
        Path result = resolveAndValidatePath(base, "file.txt");
        assertEquals(base.toRealPath().resolve("file.txt"), result);
    }

    @Test
    void existingFile_isResolvedToRealPath() throws IOException {
        Files.createFile(base.resolve("data.bin"));
        Path result = resolveAndValidatePath(base, "data.bin");
        assertTrue(Files.exists(result));
        assertTrue(result.startsWith(base.toRealPath()));
    }

    @Test
    void nestedRelativePath_isAllowed() throws IOException {
        Files.createDirectories(base.resolve("a/b"));
        Path result = resolveAndValidatePath(base, "a/b/file.txt");
        assertEquals(base.toRealPath().resolve("a/b/file.txt"), result);
    }

    @Test
    void nonExistentNestedPath_underExistingBase_isAllowed() throws IOException {
        // parent dirs don't exist yet — walk-up loop must land on base itself
        Path result = resolveAndValidatePath(base, "new/deep/dir/file.txt");
        assertEquals(base.toRealPath().resolve("new/deep/dir/file.txt"), result);
    }

    @Test
    void internalDotDot_thatStaysInside_isAllowed() throws IOException {
        Files.createDirectories(base.resolve("a"));
        Path result = resolveAndValidatePath(base, "a/../file.txt");
        assertEquals(base.toRealPath().resolve("file.txt"), result);
    }

    // ---------- traversal attempts ----------

    @Test
    void parentTraversal_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "../evil.txt"));
    }

    @Test
    void deepTraversal_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "a/../../evil.txt"));
    }

    @Test
    void traversalToSiblingWithSamePrefix_isRejected() throws IOException {
        // guards against string-based startsWith bugs: /base-evil vs /base
        Files.createDirectory(sandbox.resolve("base-evil"));
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "../base-evil/x.txt"));
    }

    @Test
    void absolutePath_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, outside.resolve("x").toString()));
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void driveRelativePath_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "C:evil.txt"));
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void backslashTraversal_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "..\\evil.txt"));
    }

    // ---------- base-dir edge cases ----------

    @Test
    void emptyPath_isAllowed() throws IOException {
        assertEquals(base.toRealPath(), resolveAndValidatePath(base, ""));
    }

    @Test
    void dotPath_isAllowed() throws IOException {
        assertEquals(base.toRealPath(), resolveAndValidatePath(base, "."));
    }

    @Test
    void nonExistentBaseDir_isRejected() {
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(sandbox.resolve("missing"), "file.txt"));
    }

    @Test
    void fileAsBaseDir_isRejected() throws IOException {
        Path file = Files.createFile(sandbox.resolve("notadir"));
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(file, "file.txt"));
    }

    @Test
    void nulByte_isRejected() {
        // InvalidPathException must surface as IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "file\u0000.txt"));
    }

    @Test
    void symlinkAtTarget_pointingOutside_isRejected() throws IOException {
        Path escapeTarget = Files.createFile(outside.resolve("secret.txt"));
        Files.createSymbolicLink(base.resolve("link.txt"), escapeTarget);
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "link.txt"));
    }

    @Test
    void danglingSymlinkAtTarget_isRejected() throws IOException {
        Files.createSymbolicLink(base.resolve("dangling"), outside.resolve("does-not-exist-yet"));
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "dangling"));
    }

    @Test
    void symlinkedDirectory_escapingViaExistingParent_isRejected() throws IOException {
        Files.createSymbolicLink(base.resolve("linkdir"), outside);
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "linkdir/file.txt"));
    }

    @Test
    void symlinkedAncestor_withNonExistentIntermediates_isRejected() throws IOException {
        // the gap in the original implementation: base/linkdir -> outside,
        // and base/linkdir/sub does not exist, so only walking ALL ancestors catches it
        Files.createSymbolicLink(base.resolve("linkdir"), outside);
        assertThrows(IllegalArgumentException.class, () -> resolveAndValidatePath(base, "linkdir/sub/file.txt"));
    }

    @Test
    void symlinkInsideBase_pointingInsideBase_isAllowed() throws IOException {
        Path realDir = Files.createDirectory(base.resolve("real"));
        Files.createSymbolicLink(base.resolve("alias"), realDir);
        Path result = resolveAndValidatePath(base, "alias/file.txt");
        assertEquals(base.toRealPath().resolve("real/file.txt"), result);
    }

    @Test
    void baseDirItselfBeingSymlink_isSupported() throws IOException {
        // operators commonly point config at a symlinked dir; base is realpath'd first
        Path linkedBase = Files.createSymbolicLink(sandbox.resolve("baselink"), base);
        Path result = resolveAndValidatePath(linkedBase, "file.txt");
        assertEquals(base.toRealPath().resolve("file.txt"), result);
        assumeTrue(Files.isSymbolicLink(linkedBase));
    }

    @Test
    public void when_plainFileName_then_resolvesInsideBase() throws IOException {
        Path resolved = IOUtil.resolveInDir(base, "2026-07-22-0-0");
        assertEquals(base.toRealPath().resolve("2026-07-22-0-0"), resolved);
    }

    @Test
    public void when_nameIsDotDotPrefixed_then_allowed() throws IOException {
        // "..foo" is a valid file name, not a traversal
        Path resolved = IOUtil.resolveInDir(base, "..foo");
        assertEquals(base.toRealPath(), resolved.getParent());
    }

    @Test
    public void when_simpleTraversal_then_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> IOUtil.resolveInDir(base, "../escaped-0"));
    }

    @Test
    public void when_nestedTraversal_then_throws() {
        // goes into a subdir first, then climbs out twice
        assertThrows(IllegalArgumentException.class,
                () -> IOUtil.resolveInDir(base, "sub/../../escaped-0"));
    }
}
