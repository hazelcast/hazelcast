package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.util.Iterator;
import java.util.Queue;

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;

public class AsyncBackupOverloadDetector {

    private final DetectionThread thread;
    private final Node node;
    private volatile boolean stop;
    private long lowWaterMark = Long.getLong("hazelcast.asyncbackup.lowwatermark", 1024 * 1024);
    private long highWaterMark = Long.getLong("hazelcast.asyncbackup.highwatermark", 5 * 1024 * 1024);
    private long durationMs = Long.getLong("hazelcast.asyncbackup.duration", 5000);
    private volatile long timeoutMs = currentTimeMillis();
    private int ratio;

    public AsyncBackupOverloadDetector(Node node) {
        this.node = node;
        this.thread = new DetectionThread();
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        stop = true;
        thread.interrupt();
    }

    public int getRatio() {
        if (currentTimeMillis() > timeoutMs) {
            return 0;
        }

        return ratio;
    }

    private class DetectionThread extends Thread {
        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }

                long startMs = System.currentTimeMillis();

                long max = maxPending();
                System.out.println("pending:" + max);
                if (max < lowWaterMark) {
                    continue;
                }

                // overload detected.
                timeoutMs = System.currentTimeMillis() + durationMs;
                max = min(max, highWaterMark);
                ratio = Math.round(100 * ((float) max) / highWaterMark);
                long duration = System.currentTimeMillis() - startMs;
                System.out.println("ratio:" + ratio + " max:" + max + " duration:" + duration + " ms");
            }
        }
    }

    private long maxPending() {
        long max = 0;
        for (Iterator<Channel> it = node.networkingService.getNetworking().channels(); it.hasNext(); ) {
            Channel channel = it.next();
            TcpIpConnection c = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
            if (c.isClient()) {
                continue;
            }

            Queue<OutboundFrame> writeQueue = ((NioChannel) channel).outboundPipeline().writeQueue;
            long pending = 0;
            for (OutboundFrame frame : writeQueue) {
                if (frame instanceof Packet) {
                    pending += frame.getFrameLength();
                }

                //optimization
                if (pending > highWaterMark) {
                    break;
                }
            }

            if (pending > max) {
                max = pending;
            }

            // optimization
            if (max > highWaterMark) {
                break;
            }
        }
        return max;
    }
}
