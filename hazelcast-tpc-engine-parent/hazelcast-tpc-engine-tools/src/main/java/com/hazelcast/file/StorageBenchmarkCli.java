package com.hazelcast.file;

import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.util.OS;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.Arrays;
import java.util.Locale;

import static com.hazelcast.CliUtils.getToolsVersion;
import static com.hazelcast.CliUtils.printHelp;
import static com.hazelcast.GitInfo.getBuildTime;
import static com.hazelcast.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.file.StorageBenchmark.READWRITE_NOP;
import static com.hazelcast.file.StorageBenchmark.READWRITE_RANDREAD;
import static com.hazelcast.file.StorageBenchmark.READWRITE_RANDWRITE;
import static com.hazelcast.file.StorageBenchmark.READWRITE_READ;
import static com.hazelcast.file.StorageBenchmark.READWRITE_WRITE;
import static com.hazelcast.internal.tpcengine.ReactorType.IOURING;

public class StorageBenchmarkCli {
    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> ioDepthSpec = parser
            .accepts("iodepth", "The io depth. AKA: The concurrency per reactor.")
            .withRequiredArg().ofType(Integer.class)
            .defaultsTo(1);

    private final OptionSpec<Integer> numJobsSpec = parser
            .accepts("numjobs", "The number of parallel jobs. AKA: The number of reactors.")
            .withRequiredArg().ofType(Integer.class)
            .defaultsTo(1);

    private final OptionSpec<Integer> bsSpec = parser
            .accepts("bs", "The block size in bytes")
            .withRequiredArg().ofType(Integer.class)
            .defaultsTo(OS.pageSize());

    private final OptionSpec<Boolean> directSpec = parser
            .accepts("direct", "True to use Direct I/O, false for Buffered I/O (page cache)")
            .withRequiredArg().ofType(Boolean.class)
            .defaultsTo(false);

    private final OptionSpec<String> directorySpec = parser
            .accepts("directory", "The directory where fio will create the benchmark files. "
                    + "To pass multiple directories, seperate them using a :")
            .withRequiredArg().ofType(String.class)
            .defaultsTo(System.getProperty("user.dir"));

    private final OptionSpec<String> readwriteSpec = parser
            .accepts("readwrite", "The workload (read, write, randread, randwrite, nop)")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("read");

    private final OptionSpec<String> runtimeSpec = parser
            .accepts("runtime", "The duration of the test, e.g. 60s")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("60s");

    private final OptionSpec<String> reactorTypeSpec = parser
            .accepts("reactortype", "The type of reactor (either iouring or nio)")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("iouring");

    public static void main(String[] args) {
        StorageBenchmarkCli cli = new StorageBenchmarkCli();
        cli.run(args);
    }

    private void run(String[] args) {
        System.out.println("StorageBenchmark");

        System.out.printf("Version: %s, Commit: %s, Build Time: %s%n",
                getToolsVersion(), getCommitIdAbbrev(), getBuildTime());

        OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
        OptionSet options = parser.parse(args);

        if (options.has(helpSpec)) {
            printHelp(parser, System.out);
            return;
        }

        StorageBenchmark benchmark = new StorageBenchmark();
        benchmark.affinity = "1";
        benchmark.numJobs = options.valueOf(numJobsSpec);
        benchmark.iodepth = options.valueOf(ioDepthSpec);
        benchmark.fileSize = 4 * 1024 * 1024L;
        benchmark.bs = options.valueOf(bsSpec);
        benchmark.directories.addAll(Arrays.asList(options.valueOf(directorySpec).split(":")));

        String readwrite = options.valueOf(readwriteSpec);
        if ("read".equals(readwrite)) {
            benchmark.readwrite = READWRITE_READ;
        } else if ("write".equals(readwrite)) {
            benchmark.readwrite = READWRITE_WRITE;
        } else if ("randread".equals(readwrite)) {
            benchmark.readwrite = READWRITE_RANDREAD;
        } else if ("randwrite".equals(readwrite)) {
            benchmark.readwrite = READWRITE_RANDWRITE;
        } else if ("nop".equals(readwrite)) {
            benchmark.readwrite = READWRITE_NOP;
        } else {
            System.out.println("Unrecognized readwrite value [" + readwrite + "]");
        }

        String runtime = options.valueOf(runtimeSpec);
        if (!runtime.endsWith("s")) {
            System.out.println("Runtime needs to end with 's'");
        }

        String runtimeSec = runtime.substring(0, runtime.length() - 1);
        benchmark.runtimeSeconds = Integer.parseInt(runtimeSec);
        benchmark.deleteFilesOnExit = true;
        benchmark.direct = options.valueOf(directSpec);
        benchmark.spin = false;
        benchmark.reactorType = ReactorType.fromString(options.valueOf(reactorTypeSpec));
        benchmark.fsync = 0;
        benchmark.fdatasync = 0;
        benchmark.run();
        System.exit(0);
    }
}
