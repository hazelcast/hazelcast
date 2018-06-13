package com.hazelcast.test.classhistogram;

import com.hazelcast.test.process.ProcessUtils;
import com.hazelcast.test.process.ExecutionResult;

import java.io.IOException;

final class ClassHistogram {
    private static final boolean DEBUG = Boolean.getBoolean("hazelcast.test.debug.class.histogram");

    private ClassHistogram() {

    }

    /**
     * Dump class histogram to standard output
     *
     * @param maxLines maximum number of lines to be written. If histogram has more lines then it'll be truncated
     */
    static void dump(int maxLines) {
        Long processIdOrNull = ProcessUtils.getOwnPidOrNull();
        if (processIdOrNull == null) {
            System.out.println("Cannot find own process id.");
            return;
        }

        try {
            ExecutionResult result = executeHistogramTool(maxLines, processIdOrNull);
            ProcessUtils.printResult("Class Histogram", result);
        } catch (IOException e) {
            System.out.println("Cannot find histogram generator.");
            if (DEBUG) {
                e.printStackTrace();
            }
        }

    }

    private static ExecutionResult executeHistogramTool(int maxLines, long processId) throws IOException {
        try {
            return ProcessUtils.executeBlocking(maxLines, "jcmd", Long.toString(processId), "GC.class_histogram");
        } catch (IOException e) {
            if (DEBUG) {
                e.printStackTrace();
            }
            //fallback to jmap. this is an older tool, jcmd is prefered nowdays
            return ProcessUtils.executeBlocking(maxLines, "jmap", "-histo:live", Long.toString(processId));
        }
    }
}
