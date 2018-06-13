package com.hazelcast.test.process;

public final class ExecutionResult {
    private final int exitCode;
    private final String sysout;
    private final String errout;

    ExecutionResult(int exitCode, String sysout, String errout) {
        this.exitCode = exitCode;
        this.sysout = sysout;
        this.errout = errout;
    }

    public int getExitCode() {
        return exitCode;
    }

    public String getSysout() {
        return sysout;
    }

    public String getErrout() {
        return errout;
    }
}
