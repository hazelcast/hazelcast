/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.instance.impl.BootstrappedInstanceProxy;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class CommandLineExecuteJar {

    // Use JUL Logging, not Hazelcast logging
    private static final Logger LOGGER = Logger.getLogger(CommandLineExecuteJar.class.getName());

    // The number of attempts to check if job is starting
    private static final int JOB_START_CHECK_ATTEMPTS = 10;

    // The number of seconds to sleep between each checks
    private static final int JOB_START_CHECK_INTERVAL_MILLIS = 1_000;

    private static final EnumSet<JobStatus> STARTUP_STATUSES = EnumSet.of(NOT_RUNNING, STARTING);

    /**
     * This method is used by a client to execute a jar in a single threaded environment
     * <p>
     * The startup of the job is awaited for some period of time before this method returns.
     * Then the HZ client is closed
     */
    public void executeJar(@Nonnull ResettableSingleton<BootstrappedInstanceProxy> singleton,
                           ExecuteJobParameters executeJobParameters,
                           @Nullable String mainClassName,
                           @Nonnull List<String> args
    ) throws IOException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        BootstrappedInstanceProxy instanceProxy = singleton.remembered();
        boolean exit = false;
        try {
            String jarPath = executeJobParameters.getJarPath();
            mainClassName = ExecuteJarHelper.findMainClassNameForJar(jarPath, mainClassName);

            URL jarUrl = new File(jarPath).toURI().toURL();
            try (URLClassLoader classLoader = URLClassLoader.newInstance(
                    new URL[]{jarUrl},
                    CommandLineExecuteJar.class.getClassLoader())) {

                Method mainMethod = ExecuteJarHelper.findMainMethodForJar(classLoader, mainClassName);

                LOGGER.info("Found mainClassName :\"" + mainClassName + "\" and main method");

                invokeMain(instanceProxy, executeJobParameters, mainMethod, args);
            }
            // Wait for the job to start
            awaitJobsStartedByJar(instanceProxy);

        } catch (JetException exception) {
            // Only JetException causes exit code. Other exceptions such as ClassNotFound etc. are ignored
            LOGGER.log(Level.SEVERE, "JetException caught while executing the jar ", exception);
            exit = true;
        } catch (Exception exception) {
            LOGGER.log(Level.SEVERE, "Exception caught while executing the jar ", exception);
            throw exception;
        } finally {
            try {
                instanceProxy.shutdown();
            } catch (Exception exception) {
                LOGGER.log(Level.SEVERE, "Shutdown failed with:", exception);
            }
            singleton.resetRemembered();
            if (exit) {
                System.exit(1);
            }
        }
    }

    private void invokeMain(BootstrappedInstanceProxy instanceProxy, ExecuteJobParameters executeJobParameters,
                            Method mainMethod, List<String> args)
            throws IllegalAccessException, InvocationTargetException {
        try {
            instanceProxy.setExecuteJobParameters(executeJobParameters);

            String[] jobArgs = args.toArray(new String[0]);

            // upcast args to Object, so it's passed as a single array-typed argument
            mainMethod.invoke(null, (Object) jobArgs);
        } finally {
            instanceProxy.removeExecuteJobParameters();
        }
    }

    private void awaitJobsStartedByJar(BootstrappedInstanceProxy instanceProxy) {
        List<Job> submittedJobs = instanceProxy.getSubmittedJobs();
        if (submittedJobs.isEmpty()) {
            LOGGER.severe("The JAR didn't submit any jobs.");
            return;
        }

        int previousStartingJobCount = -1;

        for (int index = 0; index < JOB_START_CHECK_ATTEMPTS; index++) {
            // Sleep and wait for the jobs to start
            uncheckRun(() -> Thread.sleep(JOB_START_CHECK_INTERVAL_MILLIS));

            List<Job> startedJobs = submittedJobs.stream()
                    .filter(job -> !STARTUP_STATUSES.contains(job.getStatus()))
                    .collect(Collectors.toList());

            List<Job> startingJobs = submittedJobs.stream()
                    .filter(job -> !startedJobs.contains(job))
                    .collect(Collectors.toList());

            int startingJobCount = startingJobs.size();

            // No starting jobs. startingJobCount was the same in the previous run
            if (startingJobs.isEmpty() && noChangeInStartingJobCount(previousStartingJobCount, startingJobCount)) {
                break;
            }

            // If starting job count has changed, log jobs
            if (changeInStartingJobCount(previousStartingJobCount, startingJobCount)) {
                logJobs(startingJobs);
                logRemainingCount(startingJobCount);
            }
            previousStartingJobCount = startingJobCount;
        }
    }

    private boolean noChangeInStartingJobCount(int previousStartingJobCount, int startingJobCount) {
        return startingJobCount == previousStartingJobCount;
    }

    private boolean changeInStartingJobCount(int previousStartingJobCount, int startingJobCount) {
        return startingJobCount != previousStartingJobCount;
    }

    private void logJobs(List<Job> startingJobs) {
        for (Job job : startingJobs) {
            // The change of job statuses after the check above
            // won't be a problem here. Because they cannot revert
            // back to startup statuses.
            String nameOrId = Objects.toString(job.getName(), job.getIdString());

            String message = String.format("Job '%s' submitted at %s changed status to %s at %s.",
                    nameOrId,
                    toLocalDateTime(job.getSubmissionTime()),
                    job.getStatus(),
                    toLocalDateTime(System.currentTimeMillis()));
            LOGGER.warning(message);
        }
    }

    private void logRemainingCount(int startingJobCount) {
        if (startingJobCount == 1) {
            LOGGER.warning("A job is still starting...");
        } else if (startingJobCount > 1) {
            String message = String.format("%,d jobs are still starting...%n", startingJobCount);
            LOGGER.warning(message);
        }
    }
}
