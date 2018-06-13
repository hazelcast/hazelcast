package com.hazelcast.test.classhistogram;

import com.hazelcast.test.process.ProcessUtils;
import com.hazelcast.test.process.ExecutionResult;

import java.io.IOException;

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

public class ClassHistogram {
    private static final boolean DEBUG = Boolean.getBoolean("hazelcast.test.debug.class.histogram");

    /**
     * Dump class histogram to standard output
     *
     * @param maxLines maximum number of lines to be written. If histogram has more lines then it'll be truncated
     */
    public static void dump(int maxLines) {
        Long processIdOrNull = ProcessUtils.getOwnPidOrNull();
        if (processIdOrNull == null) {
            System.out.println("Cannot find own process id.");
            return;
        }

        ExecutionResult result;
        try {
            result = executeHistogramTool(maxLines, processIdOrNull);
        } catch (IOException e) {
            System.out.println("Cannot find histogram generator.");
            if (DEBUG) {
                e.printStackTrace();
            }
            return;
        }

        if (result.getExitCode() == 0) {
            System.out.println("Class Histogram: " + LINE_SEPARATOR + result.getSysout());
        } else {
            System.out.println("Error while generating class histogram. Exit code: " + result.getExitCode()
                    + " Standard output: " + LINE_SEPARATOR + result.getSysout()
                    + " Error output " + LINE_SEPARATOR + result.getErrout());
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
