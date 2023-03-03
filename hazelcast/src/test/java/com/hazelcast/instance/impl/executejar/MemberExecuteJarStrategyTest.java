package com.hazelcast.instance.impl.executejar;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.BootstrappedInstanceProxy;
import com.hazelcast.instance.impl.BootstrappedJetProxy;
import com.hazelcast.jet.impl.AbstractJetInstance;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

public class MemberExecuteJarStrategyTest {

    @Test
    public void testInvokeMain() throws InvocationTargetException, IllegalAccessException {
        HazelcastInstance hazelcastInstance = Mockito.mock(HazelcastInstance.class);
        AbstractJetInstance abstractJetInstance = Mockito.mock(AbstractJetInstance.class);
        Mockito.when(hazelcastInstance.getJet()).thenReturn(abstractJetInstance);
        Mockito.when(abstractJetInstance.getHazelcastInstance()).thenReturn(hazelcastInstance);

        BootstrappedInstanceProxy instanceProxy = BootstrappedInstanceProxy.createWithJetProxy(hazelcastInstance);

        Method method = Mockito.mock(Method.class);

        MemberExecuteJarStrategy memberExecuteJarStrategy = new MemberExecuteJarStrategy();

        String jarPath = "jarPath";
        String snapshotName = "snapshotName";
        String jobName = "jobName";
        memberExecuteJarStrategy.invokeMain(instanceProxy,
                jarPath,
                snapshotName,
                jobName,
                method,
                new String[]{"jobArgs"}
        );

        BootstrappedJetProxy bootstrappedJetProxy = instanceProxy.getJet();
        assertThat(bootstrappedJetProxy.getJar()).isEqualTo(jarPath);
        assertThat(bootstrappedJetProxy.getSnapshotName()).isEqualTo(snapshotName);
        assertThat(bootstrappedJetProxy.getJobName()).isEqualTo(jobName);
    }
}
