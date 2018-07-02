package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.metrics.CompressingProbeRenderer;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

public class Main extends HazelcastTestSupport {

    public static void main(String[] args) throws Exception{
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();


        for(int k=0;k<1000;k++){
            hz.getMap("foobar").put("1", "1");
        }
        hz.getExecutorService("foo").execute(new MyTask());

        Node node = getNode(hz);

        Thread.sleep(10000);

        MetricsRegistry registry = node.nodeEngine.getMetricsRegistry();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        DataOutputStream printWriter = new DataOutputStream(outputStream);
//        printWriter.writeLong(System.currentTimeMillis());
//        printWriter.writeUTF(node.address.toString());
//        printWriter.writeUTF(node.config.getGroupConfig().getName());
//        printWriter.flush();

        CompressingProbeRenderer probeRenderer = new CompressingProbeRenderer(10000);
        registry.render(probeRenderer);
        outputStream.write(probeRenderer.getRenderedBlob());
        //probeRenderer.flush();
        outputStream.flush();


        byte[] bytes = outputStream.toByteArray();
        System.out.println(bytes.length);
    }

    public static class MyTask implements Runnable, Serializable{
        @Override
        public void run() {
        }
    }
}
