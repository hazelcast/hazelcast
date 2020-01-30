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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.StringJoiner;

/**
 * Configuration object for the Python service factory.
 */
public class PythonServiceConfig implements Serializable {
    private File baseDir;
    private File handlerFile;
    private String handlerModule;
    private String handlerFunction;

    /**
     * Validates the configuration and throws an exception of
     * a mandatory config option is missing.
     */
    public void validate() {
        StringJoiner missingMandatoryFields = new StringJoiner(", ");
        if (baseDir == null) {
            if (handlerFile == null) {
                missingMandatoryFields.add("(baseDir or handlerFile)");
            }
        } else if (handlerModule == null) {
            missingMandatoryFields.add("handlerModule");
        }
        if (handlerFunction == null) {
            missingMandatoryFields.add("handlerFunction");
        }
        if (missingMandatoryFields.length() > 0) {
            throw new InvalidPythonServiceConfigException("The supplied Python Service configuration is missing these " +
                    "mandatory fields: " + missingMandatoryFields);
        }
    }

    /**
     * Returns the Python {@linkplain #setBaseDir base directory}.
     */
    @Nullable
    public File baseDir() {
        return baseDir;
    }

    /**
     * Sets the base directory where the Python files reside. When you set this,
     * also set the name of the {@link #setHandlerModule handler module} to
     * identify the location of the handler function.
     * <p>
     * Jet also recognizes these special files in the base directory:
     * <ul><li>
     *     {@code requirements.txt} is assumed to list the <a href=
     *     "https://pip.pypa.io/en/stable/user_guide/#requirements-files">
     *     dependencies of your Python code</a>. Jet will automatically install
     *     them to a job-local virtual environment. You can also install the
     *     modules to the Jet servers' global Python environment in order to speed
     *     up job initialization. Jet reuses the global modules and adds the
     *     missing ones.
     * <li>
     *     {@code init.sh} is assumed to be a Bash script that Jet will run when
     *     initializing the job.
     * <li>
     *     {@code cleanup.sh} is assumed to be a Bash script that Jet will run
     *     when completing the job.
     * </ul>
     * <p>
     * If all you need to deploy to Jet is in a single file, you can call {@link
     * #setHandlerFile} instead.
     */
    public PythonServiceConfig setBaseDir(@Nonnull String baseDir) {
        if (handlerFile != null) {
            throw new IllegalArgumentException(
                    "You already set handlerFile so you can't set baseDir." +
                    " When using baseDir, set handlerModule instead.");
        }
        String baseDirStr = requireNonBlank(baseDir, "baseDir");
        try {
            File dir = new File(baseDirStr).getCanonicalFile();
            if (!dir.isDirectory()) {
                throw new IOException("Not a directory: " + dir);
            }
            this.baseDir = dir;
        } catch (IOException e) {
            throw new InvalidPythonServiceConfigException("Invalid baseDir argument", e);
        }
        return this;
    }

    /**
     * Returns the Python {@linkplain #setHandlerFile handler file}.
     */
    @Nullable
    public File handlerFile() {
        return handlerFile;
    }

    /**
     * Sets the Python handler file. It must contain the {@linkplain
     * #setHandlerFunction handler function}. If your Python work is in more
     * than one file, call {@link #setBaseDir} instead.
     */
    public PythonServiceConfig setHandlerFile(@Nonnull String handlerFile) {
        if (baseDir != null) {
            throw new IllegalStateException(
                    "You already set baseDir so you can't set handlerFile." +
                    " If you want to set the handler module, call setHandlerModule().");
        }
        if (handlerModule != null) {
            throw new IllegalStateException(
                    "You already set handlerModule, it would be overwritten by setting handlerFile");
        }
        String handlerFileStr = requireNonBlank(handlerFile, "handlerFile");
        if (!handlerFileStr.toLowerCase().endsWith(".py")) {
            throw new IllegalArgumentException("The handler file must be a .py file");
        }
        try {
            File file = new File(handlerFileStr).getCanonicalFile();
            if (!file.isFile()) {
                throw new IOException("Not a regular file: " + file);
            }
            this.handlerFile = file;
            this.handlerModule = file.getName().replaceFirst("\\.py$", "");
        } catch (IOException e) {
            throw new InvalidPythonServiceConfigException("Invalid handlerFile argument", e);
        }
        return this;
    }

    /**
     * Returns the {@linkplain #setHandlerModule handler module} name.
     * */
    public String handlerModule() {
        return handlerModule;
    }

    /**
     * Sets the name of the Python module that has the function that
     * transforms Jet pipeline data.
     */
    public PythonServiceConfig setHandlerModule(@Nonnull String handlerModule) {
        if (handlerFile != null) {
            throw new IllegalStateException(
                    "You already set handlerFile, it would be overwritten by setting handlerModule");
        }
        this.handlerModule = requireNonBlank(handlerModule, "handlerModule");
        return this;
    }

    /**
     * Returns the name of the {@linkplain #setHandlerFunction handler
     * function}.
     */
    public String handlerFunction() {
        return handlerFunction;
    }

    /**
     * Sets the name of the Python function that transforms Jet pipeline data,
     * defined in the module you configured with {@link #setHandlerModule}.
     * The function must take a single argument that is a list of strings, and
     * return another list of strings which has the results of transforming each
     * item in the input list. There must be a strict one-to-one match between
     * the input and output lists.
     * <p>
     * Here's a simple example of a handler function that transforms every input
     * string by prepending {@code "echo-"} to it:
     * <pre>
     * def handle(input_list):
     *     return ["echo-%s" % i for i in input_list]
     * </pre>
     */
    public PythonServiceConfig setHandlerFunction(@Nonnull String handlerFunction) {
        this.handlerFunction = requireNonBlank(handlerFunction, "handlerFunction");
        return this;
    }

    private static String requireNonBlank(@Nonnull String in, @Nonnull String name) {
        in = in.trim();
        if (in.isEmpty()) {
            throw new IllegalArgumentException("Parameter must not be blank: " + name);
        }
        return in;
    }
}
