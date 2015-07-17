package com.hazelcast.internal.metrics.renderers;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HumanFriendlyProbeRendererTest extends HazelcastTestSupport {

    private HumanFriendlyProbeRenderer renderer;

    @Before
    public void setup() {
        renderer = new HumanFriendlyProbeRenderer();
    }

    @Test
    public void test() {
        renderer.start();
        renderer.renderLong("long", 1);
        renderer.renderDouble("double", 2);
        renderer.renderException("exception", new RuntimeException("somemessage"));
        renderer.renderNoValue("novalue");
        renderer.finish();

        String s = renderer.getResult();

        assertEquals("long=1\n" +
                "double=2.0\n" +
                "exception=somemessage\n" +
                "novalue=NA\n", s);
    }

}
