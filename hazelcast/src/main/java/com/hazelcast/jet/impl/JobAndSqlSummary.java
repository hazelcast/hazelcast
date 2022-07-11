package com.hazelcast.jet.impl;

public class JobAndSqlSummary {
    private JobSummary jobSummary;
    private SqlSummary sqlSummary;

    public JobAndSqlSummary() {
    }

    public JobAndSqlSummary(JobSummary jobSummary, SqlSummary sqlSummary) {
        this.jobSummary = jobSummary;
        this.sqlSummary = sqlSummary;
    }

    public long getJobId() {
        return jobSummary.getJobId();
    }

    public long getSubmissionTime() {
        return jobSummary.getSubmissionTime();
    }

    public JobSummary getJobSummary() {
        return jobSummary;
    }

    public SqlSummary getSqlSummary() {
        return sqlSummary;
    }

    public void setJobSummary(JobSummary jobSummary) {
        this.jobSummary = jobSummary;
    }

    public void setSqlSummary(SqlSummary sqlSummary) {
        this.sqlSummary = sqlSummary;
    }

    @Override
    public String toString() {
        return "JobAndSqlSummary{" +
                "jobSummary=" + jobSummary +
                ", sqlSummary=" + sqlSummary +
                '}';
    }
}
