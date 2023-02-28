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

package com.hazelcast.instance.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

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
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * This class shouldn't be directly used, instead see {@link Hazelcast#bootstrappedInstance()}
 * for the replacement and docs.
 * <p>
 * A helper class that allows one to create a standalone runnable JAR which
 * contains all the code needed to submit a job to a running Hazelcast cluster.
 * The main issue with achieving this is that the JAR must be attached as a
 * resource to the job being submitted, so the Hazelcast cluster will be able
 * to load and use its classes. However, from within a running {@code main()}
 * method it is not trivial to find out the filename of the JAR containing
 * it.
 **/
public final class ClientExecuteJarStrategy {

    private static final ILogger LOGGER = Logger.getLogger(ClientExecuteJarStrategy.class.getName());
    private static final int JOB_START_CHECK_INTERVAL_MILLIS = 1_000;
    private static final EnumSet<JobStatus> STARTUP_STATUSES = EnumSet.of(NOT_RUNNING, STARTING);

    public synchronized void executeJar(@Nonnull ConcurrentMemoizingSupplier<BootstrappedInstanceProxy> singleton,
                                        @Nonnull String jarPath,
                                        @Nullable String snapshotName,
                                        @Nullable String jobName,
                                        @Nullable String mainClassName,
                                        @Nonnull List<String> args
    ) throws IOException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        try {
            mainClassName = findMainClassNameForJar(mainClassName, jarPath);

            URL jarUrl = new File(jarPath).toURI().toURL();
            try (URLClassLoader classLoader = URLClassLoader.newInstance(
                    new URL[]{jarUrl},
                    ClientExecuteJarStrategy.class.getClassLoader())) {

                Method main = findMainMethodForJar(classLoader, mainClassName);

                String[] jobArgs = args.toArray(new String[0]);

                ExecuteJarStrategyHelper.resetJetParametersOfJetProxy(singleton, jarPath, snapshotName, jobName);

                // upcast args to Object so it's passed as a single array-typed argument
                main.invoke(null, (Object) jobArgs);
            }

            // Wait for the job to start only if called by the client side

            awaitJobsStarted(singleton);


        } finally {
            HazelcastInstance remembered = singleton.remembered();
            try {
                remembered.shutdown();
            } catch (Exception exception) {
                LOGGER.severe("Shutdown failed with:", exception);
            }

            singleton.resetRemembered();
        }
    }

    static String findMainClassNameForJar(String mainClass, String jarPath)
            throws IOException {
        MainClassNameFinder mainClassNameFinder = new MainClassNameFinder();
        mainClassNameFinder.findMainClass(mainClass, jarPath);

        if (mainClassNameFinder.hasError()) {
            String errorMessage = mainClassNameFinder.getErrorMessage();
            // Exit immediately
            error(errorMessage);
        }
        return mainClassNameFinder.getMainClassName();
    }

    static Method findMainMethodForJar(ClassLoader classLoader, String mainClassName) throws ClassNotFoundException {
        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        mainMethodFinder.findMainMethod(classLoader, mainClassName);

        if (mainMethodFinder.hasError()) {
            // Exit immediately
            String errorMessage = mainMethodFinder.getErrorMessage();
            error(errorMessage);
        }
        return mainMethodFinder.getMainMethod();
    }


    // Method is call by synchronized executeJar() so, it is safe to access submittedJobs array in BootstrappedJetProxy
    private void awaitJobsStarted(ConcurrentMemoizingSupplier<BootstrappedInstanceProxy> supplier) {

        List<Job> submittedJobs = supplier.remembered().getJet().submittedJobs();
        int submittedCount = submittedJobs.size();
        if (submittedCount == 0) {
            LOGGER.info("The JAR didn't submit any jobs.");
            return;
        }
        int previousCount = -1;
        while (true) {
            uncheckRun(() -> Thread.sleep(JOB_START_CHECK_INTERVAL_MILLIS));
            List<Job> startedJobs = submittedJobs.stream()
                    .filter(job -> !STARTUP_STATUSES.contains(job.getStatus()))
                    .collect(Collectors.toList());

            submittedJobs = submittedJobs.stream()
                    .filter(job -> !startedJobs.contains(job))
                    .collect(Collectors.toList());

            int remainingCount = submittedJobs.size();

            if (submittedJobs.isEmpty() && remainingCount == previousCount) {
                break;
            }
            if (remainingCount == previousCount) {
                continue;
            }
            for (Job job : startedJobs) {
                // The change of job statuses after the check above
                // won't be a problem here. Because they cannot revert
                // back to startup statuses.
                if (job.getName() != null) {
                    LOGGER.info("Job '" + job.getName() + "' submitted at "
                                + toLocalDateTime(job.getSubmissionTime()) + " changed status to "
                                + job.getStatus() + " at " + toLocalDateTime(System.currentTimeMillis()) + ".");
                } else {
                    LOGGER.info("Job '" + job.getIdString() + "' submitted at "
                                + toLocalDateTime(job.getSubmissionTime()) + " changed status to "
                                + job.getStatus() + " at " + toLocalDateTime(System.currentTimeMillis()) + ".");
                }
            }
            if (remainingCount == 1) {
                LOGGER.info("A job is still starting...");
            } else if (remainingCount > 1) {
                String message = String.format("%,d jobs are still starting...%n", remainingCount);
                LOGGER.info(message);
            }
            previousCount = remainingCount;
        }
    }

    private static void error(String msg) {
        LOGGER.severe(msg);
        System.exit(1);
    }
}
