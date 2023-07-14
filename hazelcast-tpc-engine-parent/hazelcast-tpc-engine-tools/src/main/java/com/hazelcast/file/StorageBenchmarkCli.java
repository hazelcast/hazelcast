package com.hazelcast.file;

import com.hazelcast.CliUtils;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.OS;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.hazelcast.file.StorageBenchmark.READWRITE_READ;

public class StorageBenchmarkCli {
    private final OptionParser parser = new OptionParser();
    protected final TpcLogger LOGGER = TpcLoggerLocator.getLogger(StorageBenchmarkCli.class);

    private final OptionSpec<Integer> ioDepthSpec = parser.accepts("iodepth", "The io depth. AKA: The concurrency per reactor.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(1);

    private final OptionSpec<Integer> numJobsSpec = parser.accepts("numjobs", "The number of parallel jobs. AKA: The number of reactors.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(1);

    private final OptionSpec<Integer> bsSpec = parser.accepts("bs", "The block size in bytes")
            .withRequiredArg().ofType(Integer.class).defaultsTo(OS.pageSize());

    private final OptionSpec<Integer> dirSpec = parser.accepts("bs", "The block size in bytes")
            .withRequiredArg().ofType(Integer.class).defaultsTo(OS.pageSize());

    private final OptionSpec<Boolean> directSpec = parser.accepts("direct", "True to use Direct I/O, false for Buffered I/O (page cache)")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> directorySpec = parser.accepts("directectory", "The directory where fio will create the benchmark files")
            .withRequiredArg().ofType(String.class).defaultsTo(System.getProperty("user.dir"));

    public static void main(String[] args) {
        StorageBenchmarkCli cli = new StorageBenchmarkCli();
        cli.run(args);
    }

    private void run(String[] args) {
        LOGGER.info("StorageBenchmark");
//        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
//                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
//        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));


        OptionSpec helpSpec = parser.accepts("help", "Shows the help.").forHelp();
        OptionSet options = parser.parse(args);

        if (options.has(helpSpec)) {
//            if (help != null) {
//                printHelp(help);
//            }
            CliUtils.printHelp(parser, System.out);
            return;
        }


        StorageBenchmark benchmark = new StorageBenchmark();
        benchmark.operationCount = 10 * 1000 * 1000L;
        benchmark.affinity = "1";
        benchmark.numJobs = options.valueOf(numJobsSpec);
        benchmark.iodepth = options.valueOf(ioDepthSpec);
        benchmark.fileSize = 4 * 1024 * 1024L;
        benchmark.bs = options.valueOf(bsSpec);
        benchmark.directory = options.valueOf(directorySpec);
        benchmark.readwrite = READWRITE_READ;
        benchmark.enableMonitor = true;
        benchmark.deleteFilesOnExit = true;
        benchmark.direct = options.valueOf(directSpec);
        benchmark.spin = false;
        benchmark.reactorType = ReactorType.IOURING;
        benchmark.fsync = 0;
        benchmark.fdatasync = 0;
        benchmark.run();
    }
}
