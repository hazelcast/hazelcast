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

import com.hazelcast.jet.pipeline.GeneralStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Configuration object for the Python service factory, used in a
 * {@link PythonTransforms#mapUsingPython mapUsingPython} stage.
 * <p>
 * Hazelcast Jet expects you to have a Python project in a local directory.
 * It must contain the definition of a {@code transform_list()} function
 * that receives a list of strings and returns a list of strings of the
 * same size, with a one-to-one mapping between input and output elements.
 * Here's a simple example of a function that transforms every input
 * string by prepending {@code "echo-"} to it:
 * <pre>{@code
 * def transform_list(input_list):
 *     return ["echo-%s" % i for i in input_list]
 * }</pre>
 * If you have a very simple setup with everything in a single Python file,
 * you can use {@link #setHandlerFile}. Let's say you saved the above
 * Python code to a file named {@code echo.py}. You can use it from Jet
 * like this:
 * <pre>{@code
 * StreamStage<String> inputStage = createInputStage();
 * StreamStage<String> outputStage = inputStage.apply(
 *         mapUsingPython(new PythonServiceConfig()
 *                 .setHandlerFile("path/to/echo.py")));
 * }</pre>
 * In more complex setups you can tell Jet the location of your project
 * {@linkplain #setBaseDir directory} and the name of the Python {@linkplain
 * #setHandlerModule module} containing {@code transform_list()}. You can
 * also use a {@linkplain #setHandlerFunction different name} for the
 * function.
 * <p>
 * Jet uploads the entire directory to the cluster, creates one or more
 * Python processes on each member, and sends the pipeline data through
 * your function. The number of processes is controlled by the {@linkplain
 * GeneralStage#setLocalParallelism local parallelism} of the Python
 * mapping stage.
 * <p>
 * Jet recognizes these special files in the base directory:
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
 * Regardless of local parallelism, the init and cleanup scripts run only
 * once per cluster member. They run within the context of the job-local
 * virtual Python environment.
 * <p>
 * To use this stage in a Hazelcast Jet cluster, Python must be installed
 * on every cluster member. Jet supports Python versions 3.5-3.7. If the
 * code has dependencies on non-standard Python modules, these must either
 * be pre-installed or the member machines must have access to the public
 * internet so that Jet can download and install them. A third option is
 * to write {@code init.sh} that uses a different way of installing the
 * dependencies. In that case make sure not to use the standard filename
 * {@code requirements.txt}, which Jet uses automatically.
 * <p>
 * The Python mapping stage produces log output at the {@code FINE} level
 * under the {@code com.hazelcast.jet.python} log category. This includes
 * all the output from launched subprocesses.
 *
 * @since Jet 4.0
 */
public class PythonServiceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String HANDLER_FUNCTION_DEFAULT = "transform_list";

    private File baseDir;
    private File handlerFile;
    private String handlerModule;
    private String handlerFunction = HANDLER_FUNCTION_DEFAULT;

    /**
     * Validates the configuration and throws an exception of a mandatory
     * config option is missing. Called automatically from {@link
     * PythonTransforms#mapUsingPython}.
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
     * identify the location of the handler function (named {@code
     * transform_list()} by convention).
     * <p>
     * If all you need to deploy to Jet is in a single file, you can call {@link
     * #setHandlerFile} instead.
     */
    @Nonnull
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
    @Nonnull
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
        if (!handlerFileStr.toLowerCase(Locale.ROOT).endsWith(".py")) {
            throw new IllegalArgumentException("The handler file must be a .py file");
        }
        try {
            File file = new File(handlerFileStr).getCanonicalFile();
            if (!file.isFile() || !file.canRead()) {
                throw new IOException("Not a regular, readable file: " + file);
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
    @Nullable
    public String handlerModule() {
        return handlerModule;
    }

    /**
     * Sets the name of the Python module that has the function that
     * transforms Jet pipeline data.
     */
    @Nonnull
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
     * function}. The default value is {@code transform_list}.
     */
    @Nonnull
    public String handlerFunction() {
        return handlerFunction;
    }

    /**
     * Overrides the default name of the Python function that transforms Jet
     * pipeline data. The default name is {@value #HANDLER_FUNCTION_DEFAULT}.
     * It must be defined in the module you configured with {@link
     * #setHandlerModule}, must take a single argument that is a list of
     * strings, and return another list of strings which has the results of
     * transforming each item in the input list. There must be a strict
     * one-to-one match between the input and output lists.
     */
    @Nonnull
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
