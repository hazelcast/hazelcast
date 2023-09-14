package com.hazelcast;

import com.hazelcast.internal.tpcengine.iouring.LinuxSocket;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Threads(value = 1)
public class LinuxSocketSetQuickAckBenchmark {

    private LinuxSocket socket;

    @Setup
    public void setup() {
        socket = LinuxSocket.createNonBlockingTcpIpv4Socket();
    }

    @TearDown
    public void teardown() {
        socket.close();
    }

    @Benchmark
    public void setTcpQuickAck() throws IOException {
        socket.setTcpQuickAck(true);
    }

    @Benchmark
    public boolean isTcpQuickAck() throws IOException {
        return socket.isTcpQuickAck();
    }
}
