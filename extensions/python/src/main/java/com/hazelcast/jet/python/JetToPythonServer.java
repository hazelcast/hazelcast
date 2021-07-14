/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Path;

import static com.hazelcast.jet.python.PythonService.MAIN_SHELL_SCRIPT;
import static com.hazelcast.jet.python.PythonServiceContext.logStdOut;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

class JetToPythonServer {
    private final File baseDir;
    private ILogger logger;

    private Process pythonProcess;
    private String pythonProcessPid;
    private Thread stdoutLoggingThread;

    JetToPythonServer(@Nonnull Path baseDir, @Nonnull ILogger logger) {
        this.baseDir = baseDir.toFile();
        this.logger = logger;
    }

    int start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            ProcessBuilder builder = new ProcessBuilder(
                    "/bin/sh", "-c", String.format("./%s %d", MAIN_SHELL_SCRIPT, serverSocket.getLocalPort()));
            pythonProcess = builder
                    .directory(baseDir)
                    .redirectErrorStream(true)
                    .start();
            stdoutLoggingThread = logStdOut(logger, pythonProcess, "python-main");
            pythonProcessPid = PythonServiceContext.processPid(pythonProcess);
            logger.info("Started Python process: " + pythonProcessPid);
            serverSocket.setSoTimeout((int) SECONDS.toMillis(2));
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream(), UTF_8))) {
                        int serverPort = Integer.parseInt(reader.readLine());
                        logger.info("Python process " + pythonProcessPid + " listening on port " + serverPort);
                        return serverPort;
                    }
                } catch (SocketTimeoutException e) {
                    if (!pythonProcess.isAlive()) {
                        throw new IOException("Python process died before completing initialization");
                    }
                }
            }
        }
    }

    @SuppressFBWarnings(value = "OS_OPEN_STREAM", justification = "PrintStream wraps Python's stdin, not to be closed")
    void stop() {
        try {
            new PrintStream(pythonProcess.getOutputStream(), true, UTF_8.name()).println("stop");
        } catch (UnsupportedEncodingException e) {
            logger.info("UTF_8 reported as unsupported encoding??");
        }
        boolean interrupted = false;
        while (true) {
            try {
                if (pythonProcess.waitFor(2, SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                logger.info("Ignoring interruption signal in order to prevent Python process leak");
                interrupted = true;
            }
            logger.warning(
                    "Python process " + pythonProcessPid + " still not done, sending a 'destroyForcibly' signal");
            pythonProcess.destroyForcibly();
        }
        while (true) {
            try {
                stdoutLoggingThread.join();
                break;
            } catch (InterruptedException e) {
                logger.info("Ignoring interruption signal in order to prevent thread leak (" +
                        stdoutLoggingThread.getName() + ')');
                interrupted = true;
            }
        }
        if (interrupted) {
            currentThread().interrupt();
        }
    }
}
