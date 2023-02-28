package com.hazelcast.instance.impl;

import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;

class ExecuteJarStrategyHelper {


    static void resetJetParametersOfJetProxy(ConcurrentMemoizingSupplier<BootstrappedInstanceProxy> supplier,
                                             String jar, String snapshotName, String jobName) {
        // BootstrappedInstanceProxy is a singleton that owns a HazelcastInstance and BootstrappedJetProxy
        // Change the state of the singleton
        BootstrappedInstanceProxy bootstrappedInstanceProxy = supplier.remembered();

        BootstrappedJetProxy bootstrappedJetProxy = bootstrappedInstanceProxy.getJet();
        // Clear all previously submitted jobs to avoid memory leak
        bootstrappedJetProxy.clearSubmittedJobs();
        bootstrappedJetProxy.setJarName(jar);
        bootstrappedJetProxy.setSnapshotName(snapshotName);
        bootstrappedJetProxy.setJobName(jobName);
    }
}
