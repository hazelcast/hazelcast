package com.hazelcast.test.perfcounter;

import com.hazelcast.test.process.ExecutionResult;
import com.hazelcast.test.process.ProcessUtils;

import java.io.IOException;

class PerfCounter {

    static void dump(int maxLines) {
        Long processIdOrNull = ProcessUtils.getOwnPidOrNull();
        if (processIdOrNull == null) {
            System.out.println("Cannot find own process id.");
            return;
        }

        try {
            ExecutionResult executionResult = ProcessUtils.executeBlocking(maxLines, "jcmd", Long.toString(processIdOrNull), "PerfCounter.print");
            ProcessUtils.printResult("Perf Counter", executionResult);
        } catch (IOException e) {
            System.out.println("Cannot find jcmd");
        }
    }
}
