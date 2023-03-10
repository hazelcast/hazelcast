package com.hazelcast.instance.impl.executejar;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ExecuteJobParameters {

    @Nonnull
    private String jarPath;
    @Nullable
    private String snapshotName;
    @Nullable
    private String jobName;


    public ExecuteJobParameters(@Nonnull String jarPath, @Nullable String snapshotName, @Nullable String jobName) {
        this.jarPath = jarPath;
        this.snapshotName = snapshotName;
        this.jobName = jobName;
    }

    @Nonnull
    public String getJarPath() {
        return jarPath;
    }

    public boolean hasSnapshotName() {
        return snapshotName != null;
    }

    @Nullable
    public String getSnapshotName() {
        return snapshotName;
    }

    public boolean hasJobName() {
        return jobName != null;
    }

    @Nullable
    public String getJobName() {
        return jobName;
    }
}
