/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.python;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.IOUtil.copyStream;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.Arrays.asList;

/**
 * The context object used by the "map using Python" pipeline stage. As a
 * user you don't have to deal with this class directly. It is used when
 * you write {@link PythonTransforms#mapUsingPython
 * stage.apply(PythonService.mapUsingPython(pyConfig))}
 */
class PythonServiceContext {

    private static final String JET_TO_PYTHON_PREFIX = "jet_to_python_";
    private static final String MAIN_SHELL_SCRIPT = JET_TO_PYTHON_PREFIX + "main.sh";
    private static final String PARAMS_SCRIPT = JET_TO_PYTHON_PREFIX + "params.sh";
    private static final String INIT_SHELL_SCRIPT = JET_TO_PYTHON_PREFIX + "init.sh";
    private static final String CLEANUP_SHELL_SCRIPT = JET_TO_PYTHON_PREFIX + "cleanup.sh";
    private static final String USER_INIT_SHELL_SCRIPT = "init.sh";
    private static final String USER_CLEANUP_SHELL_SCRIPT = "cleanup.sh";
    private static final String PYTHON_GRPC_SCRIPT = JET_TO_PYTHON_PREFIX + "grpc_server.py";
    private static final List<String> EXECUTABLE_SCRIPTS = asList(
            INIT_SHELL_SCRIPT, MAIN_SHELL_SCRIPT, CLEANUP_SHELL_SCRIPT);
    private static final List<String> USER_EXECUTABLE_SCRIPTS = asList(
            USER_INIT_SHELL_SCRIPT, USER_CLEANUP_SHELL_SCRIPT);
    private static final EnumSet<PosixFilePermission> WRITE_PERMISSIONS =
            EnumSet.of(OWNER_WRITE, GROUP_WRITE, OTHERS_WRITE);
    private static final Object INIT_LOCK = new Object();

    private final ILogger logger;

    private Path runtimeBaseDir;

    PythonServiceContext(ProcessorSupplier.Context context, PythonServiceConfig cfg) {
        logger = context.jetInstance().getHazelcastInstance().getLoggingService()
                        .getLogger(getClass().getPackage().getName());
        try {
            long start = System.nanoTime();
            runtimeBaseDir = cfg.baseDir() != null
                    ? context.attachedDirectory(cfg.baseDir().toString()).toPath()
                    : context.attachedFile(cfg.handlerFile().toString()).toPath().getParent();
            setupBaseDir(cfg);
            synchronized (INIT_LOCK) {
                // synchronized: the script will run pip which is not concurrency-safe
                Process initProcess = new ProcessBuilder("/bin/sh", "-c", "./" + INIT_SHELL_SCRIPT)
                        .directory(runtimeBaseDir.toFile())
                        .redirectErrorStream(true)
                        .start();
                Thread stdoutLoggingThread = logStdOut(logger, initProcess, "python-init");
                initProcess.waitFor();
                if (initProcess.exitValue() != 0) {
                    try {
                        destroy();
                    } catch (Exception e) {
                        logger.warning("Cleanup failed with exception", e);
                    }
                    throw new Exception(
                            "Initialization script finished with non-zero exit code: " + initProcess.exitValue()
                    );
                }
                stdoutLoggingThread.join();
            }
            makeFilesReadOnly(runtimeBaseDir);
            context.logger().info(String.format("Initialization script took %,d ms",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
        } catch (Exception e) {
            throw new JetException("PythonService initialization failed: " + e, e);
        }
    }

    void destroy() {
        File runtimeBaseDirF = runtimeBaseDir.toFile();
        try {
            makeFilesWritable(runtimeBaseDir);
            Path cleanupScriptPath = runtimeBaseDir.resolve(USER_CLEANUP_SHELL_SCRIPT);
            if (Files.exists(cleanupScriptPath)) {
                Process cleanupProcess = new ProcessBuilder("/bin/sh", "-c", "./" + CLEANUP_SHELL_SCRIPT)
                        .directory(runtimeBaseDirF)
                        .redirectErrorStream(true)
                        .start();
                logStdOut(logger, cleanupProcess, "python-cleanup-" + cleanupProcess);
                cleanupProcess.waitFor();
                if (cleanupProcess.exitValue() != 0) {
                    logger.warning("Cleanup script finished with non-zero exit code: " + cleanupProcess.exitValue());
                }
            }
        } catch (Exception e) {
            throw new JetException("PythonService cleanup failed: " + e, e);
        } finally {
            IOUtil.delete(runtimeBaseDir);
        }
    }

    ILogger logger() {
        return logger;
    }

    Path runtimeBaseDir() {
        return runtimeBaseDir;
    }

    private void setupBaseDir(PythonServiceConfig cfg) throws IOException {
        createParamsScript(runtimeBaseDir.resolve(PARAMS_SCRIPT),
                "HANDLER_MODULE", cfg.handlerModule(),
                "HANDLER_FUNCTION", cfg.handlerFunction()
        );
        for (String fname : asList(
                JET_TO_PYTHON_PREFIX + "pb2.py",
                JET_TO_PYTHON_PREFIX + "pb2_grpc.py",
                INIT_SHELL_SCRIPT,
                MAIN_SHELL_SCRIPT,
                CLEANUP_SHELL_SCRIPT,
                PYTHON_GRPC_SCRIPT)
        ) {
            Path destPath = runtimeBaseDir.resolve(fname);
            try (InputStream in = Objects.requireNonNull(
                    PythonServiceContext.class.getClassLoader().getResourceAsStream(fname), fname);
                 OutputStream out = Files.newOutputStream(destPath)
            ) {
                copyStream(in, out);
            }
            if (EXECUTABLE_SCRIPTS.contains(fname)) {
                makeExecutable(destPath);
            }
            for (String userScript : USER_EXECUTABLE_SCRIPTS) {
                Path scriptPath = runtimeBaseDir.resolve(userScript);
                if (Files.exists(scriptPath)) {
                    makeExecutable(scriptPath);
                }
            }
        }
    }

    static Thread logStdOut(ILogger logger, Process process, String taskName) {
        Thread thread = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
                for (String line; (line = in.readLine()) != null; ) {
                    logger.fine(line);
                }
            } catch (IOException e) {
                logger.severe("Reading init script output failed", e);
            }
        }, taskName + "-logger_" + processPid(process));
        thread.start();
        return thread;
    }

    static String processPid(Process process) {
        try {
            // Process.pid() is @since 9
            return Process.class.getMethod("pid").invoke(process).toString();
        } catch (Exception e) {
            return process.toString().replaceFirst("^.*pid=(\\d+).*$", "$1");
        }
    }

    private static void createParamsScript(@Nonnull Path paramsFile, String... namesAndVals) throws IOException {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(paramsFile))) {
            String jetToPython = JET_TO_PYTHON_PREFIX.toUpperCase();
            for (int i = 0; i < namesAndVals.length; i += 2) {
                String name = namesAndVals[i];
                String value = namesAndVals[i + 1];
                if (value != null && !value.isEmpty()) {
                    out.println(jetToPython + name + "='" + value + '\'');
                }
            }
        }
    }

    private static void makeExecutable(@Nonnull Path path) throws IOException {
        editPermissions(path, perms -> perms.add(OWNER_EXECUTE));
    }

    private void makeFilesReadOnly(@Nonnull Path basePath) throws IOException {
        editPermissionsRecursively(basePath, "-w", perms -> perms.removeAll(WRITE_PERMISSIONS));
    }

    private void makeFilesWritable(@Nonnull Path basePath) throws IOException {
        editPermissionsRecursively(basePath, "u+w", perms -> perms.add(OWNER_WRITE));
    }

    /**
     * Return value of {@code editFn} tells whether it actually changed the
     * supplied permission set. If it returns {@code false}, the file's
     * permissions won't be changed.
     */
    private static void editPermissions(
            @Nonnull Path path, @Nonnull Predicate<? super Set<PosixFilePermission>> editFn
    ) throws IOException {
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path, NOFOLLOW_LINKS);
        if (editFn.test(perms)) {
            Files.setPosixFilePermissions(path, perms);
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "https://github.com/spotbugs/spotbugs/issues/756")
    private void editPermissionsRecursively(
            @Nonnull Path basePath,
            @Nonnull String chmodOp,
            @Nonnull Predicate<? super Set<PosixFilePermission>> editFn
    ) throws IOException {
        List<String> filesNotMarked = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(basePath)) {
            walk.forEach(path -> {
                try {
                    editPermissions(path, editFn);
                } catch (Exception e) {
                    filesNotMarked.add(basePath.relativize(path).toString());
                }
            });
        }
        if (!filesNotMarked.isEmpty()) {
            logger.info("Couldn't 'chmod " + chmodOp + "' these files: " + filesNotMarked);
        }
    }
}
