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
import com.hazelcast.jet.impl.util.ResettableConcurrentMemoizingSupplier;
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
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class ClientExecuteJarStrategy {

    private static final ILogger LOGGER = Logger.getLogger(ClientExecuteJarStrategy.class.getName());
    private static final int JOB_START_CHECK_INTERVAL_MILLIS = 1_000;
    private static final EnumSet<JobStatus> STARTUP_STATUSES = EnumSet.of(NOT_RUNNING, STARTING);

    /**
     * This method needs to be fully synchronized because it shuts down the remembered object
     */
    public synchronized void executeJar(@Nonnull ResettableConcurrentMemoizingSupplier<BootstrappedInstanceProxy> singleton,
                                        @Nonnull String jarPath,
                                        @Nullable String snapshotName,
                                        @Nullable String jobName,
                                        @Nullable String mainClassName,
                                        @Nonnull List<String> args
    ) throws IOException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        BootstrappedInstanceProxy instanceProxy = singleton.remembered();
        boolean exit = false;
        try {
            mainClassName = ExecuteJarStrategyHelper.findMainClassNameForJar(mainClassName, jarPath);

            URL jarUrl = new File(jarPath).toURI().toURL();
            try (URLClassLoader classLoader = URLClassLoader.newInstance(
                    new URL[]{jarUrl},
                    ClientExecuteJarStrategy.class.getClassLoader())) {

                Method main = ExecuteJarStrategyHelper.findMainMethodForJar(classLoader, mainClassName);

                LOGGER.info("Found mainClassName :" + mainClassName + " and main method");

                String[] jobArgs = args.toArray(new String[0]);

                ExecuteJarStrategyHelper.setupJetProxy(instanceProxy, jarPath, snapshotName, jobName);

                // upcast args to Object, so it's passed as a single array-typed argument
                main.invoke(null, (Object) jobArgs);
            }

            // Wait for the job to start
            awaitJobsStarted(instanceProxy);

        } catch (JetException exception) {
            LOGGER.severe("Exception caught while executing the jar :" , exception);
            exit = true;
        } finally {
            try {
                instanceProxy.shutdown();
            } catch (Exception exception) {
                LOGGER.severe("Shutdown failed with:", exception);
            }
            singleton.resetRemembered();
            if (exit) {
                System.exit(1);
            }
        }
    }

    // suppress Reduce the total number of break and continue statements in this loop to use at most one.
    @SuppressWarnings("java:S135")
    // Method is call by synchronized executeJar() so, it is safe to access submittedJobs array in BootstrappedJetProxy
    private void awaitJobsStarted(BootstrappedInstanceProxy instanceProxy) {

        List<Job> submittedJobs = instanceProxy.getJet().submittedJobs();
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
            logJobs(startedJobs);
            logRemainingCount(remainingCount);
            previousCount = remainingCount;
        }
    }

    private void logJobs(List<Job> startedJobs) {
        for (Job job : startedJobs) {
            // The change of job statuses after the check above
            // won't be a problem here. Because they cannot revert
            // back to startup statuses.
            String nameOrId = Objects.toString(job.getName(), job.getIdString());

            String message = String.format("Job '%s ' submitted at %s changed status to %s at %s.",
                    nameOrId,
                    toLocalDateTime(job.getSubmissionTime()),
                    job.getStatus(),
                    toLocalDateTime(System.currentTimeMillis()));
            LOGGER.info(message);
        }
    }

    private void logRemainingCount(int remainingCount) {
        if (remainingCount == 1) {
            LOGGER.info("A job is still starting...");
        } else if (remainingCount > 1) {
            String message = String.format("%,d jobs are still starting...%n", remainingCount);
            LOGGER.info(message);
        }
    }
}
