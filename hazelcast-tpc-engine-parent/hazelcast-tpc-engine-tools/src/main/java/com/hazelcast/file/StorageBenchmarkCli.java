package com.hazelcast.file;

import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.util.OS;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.Arrays;

import static com.hazelcast.CliUtils.getToolsVersion;
import static com.hazelcast.CliUtils.printHelp;
import static com.hazelcast.GitInfo.getBuildTime;
import static com.hazelcast.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.file.StorageBenchmark.READWRITE_NOP;
import static com.hazelcast.file.StorageBenchmark.READWRITE_RANDREAD;
import static com.hazelcast.file.StorageBenchmark.READWRITE_RANDWRITE;
import static com.hazelcast.file.StorageBenchmark.READWRITE_READ;
import static com.hazelcast.file.StorageBenchmark.READWRITE_WRITE;

public class StorageBenchmarkCli {
    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> ioDepthSpec = parser
            .accepts("iodepth", "The io depth. AKA: The number of concurrent file I/O operations per reactor.")
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

    private final OptionSpec<Integer> filesizeSpec = parser
            .accepts("filesize", "The size of the files.")
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

    private final OptionSpec<String> ioengineSpec = parser
            .accepts("ioengine", "The type of ioengine (AKA reactortype). "
                    + "Available options: iouring or nio. "
                    + "The nio option can't be used with fio. ")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("iouring");

    private final OptionSpec<String> cpusAllowedSpec = parser
            .accepts("cpus_allowed", "The cpu affinity")
            .withRequiredArg().ofType(String.class)
            .defaultsTo(null);

    private final OptionSpec<String> cpusAllowedPolicySpec = parser
            .accepts("cpus_allowed_policy", "If the cpus from the cpus_allowed are shared or split. "
                    + "If you specify a cpus_allowed, you need to specify --cpus_allowed_policy=split"
                    + "for fio compatibility. THe 'shared' option isn't supported.")
            .withRequiredArg().ofType(String.class)
            .defaultsTo("split");

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
        benchmark.numJobs = options.valueOf(numJobsSpec);
        benchmark.iodepth = options.valueOf(ioDepthSpec);
        if(!options.has(filesizeSpec)){
            throw new RuntimeException("--filesize option is mandatory for fio compatibility.");
        }
        benchmark.fileSize = options.valueOf(filesizeSpec);
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

        if (options.has(cpusAllowedSpec)) {
            if (!"split".equals(options.valueOf(cpusAllowedPolicySpec))) {
                throw new RuntimeException("If you specify --cpus_allowed, you need to configure --cpus_allowed_policy=split. " +
                        "This is needed for fio compatibility");
            }
            benchmark.affinity = options.valueOf(cpusAllowedSpec);
        }

        String runtimeSec = runtime.substring(0, runtime.length() - 1).trim();
        benchmark.runtimeSeconds = Integer.parseInt(runtimeSec);
        benchmark.deleteFilesOnExit = true;
        benchmark.direct = options.valueOf(directSpec);
        benchmark.spin = false;
        benchmark.reactorType = ReactorType.fromString(options.valueOf(ioengineSpec));
        benchmark.fsync = 0;
        benchmark.fdatasync = 0;
        benchmark.run();
        System.exit(0);
    }
}
