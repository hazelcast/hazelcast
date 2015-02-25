

## Stabilizer.Properties File Description

The file `stabilizer.properties` is placed at the `conf` folder of your Hazelcast Stabilizer. This file is used to prepare the Stabilizer tests for their proper executions according to your business needs.

![](images/NoteSmall.jpg)***NOTE:*** *Currently, the main focuses are on the Stabilizer tests of Hazlecast on Amazon EC2 and Google Compute Engine (GCE). For the preparation of `stabilizer.properties` for GCE, please refer to the [Setting Up For GCE section](#setting-up-for-google-compute-engine). The following `stabilizer.properties` file description is mainly for Amazon EC2.*

This file includes the following parameters.

- `CLOUD_PROVIDER`: The Maven artifact ID of your cloud provider. For example, it is `aws-ec2` if you are going to test the Hazelcast on Amazon EC2. For the full list of supported clouds, please refer to [http://jclouds.apache.org/reference/providers/](http://jclouds.apache.org/reference/providers/).
- `CLOUD_IDENTITY`: The full path of the file containing your AWS access key.
- `CLOUD_CREDENTIAL`: The full path of the file containing your AWS secret key. 
- `CLOUD_POLL_INITIAL_PERIOD`: The time in milliseconds between the requests (polls) from JClouds to your cloud. Its default value is `50`.
- `CLOUD_POLL_MAX_PERIOD`: The maximum time in milliseconds between the polls to your cloud. Its default value is `1000`.
- `CLOUD_BATCH_SIZE`: The number of machines to be started/terminated in one go. For Amazon EC2, its acceptable value is `20`.
- `GROUP_NAME`: The prefix for the agent name. You may want to give different names for different test clusters. For GCE, you need to be very careful using multiple group-names, since for every port and every group name, a firewall rule is made and you can only have 100 firewall rules. If the name contains `${username}`, this section will be replaced by the actual user that runs the test. This makes it very easy to identify which user owns a certain machine.
- `USER`: The name of the user on your local machine. JClouds automatically creates a new user on the remote machine with this name as the login name. It also copies the public key of your system to the remote machine and add it to the file `~/.ssh/authorized_keys`. Therefore, once the instance is created, you can login with the command `ssh <USER>@<IP address>`. Its default value is `stabilizer`.
- `SSH_OPTIONS`: The options added to SSH. You do not need to change these options.
- `SECURITY_GROUP`: The name of the security group that includes the instances created for the Stabilizer test. For Amazon EC2, this group will be created automatically if it does not exist. If you do not specify a region for the parameter `MACHINE_SPEC` (using the `locationId` attribute), the region will be `us-east-1`. If there is an already existing security group, please make sure the ports 22, 9000, 9001 and the ports between 5701 and 5751 are open. For GCE, this parameter is not used.
- `SUBNET_ID`: The VPC Subnet ID for Amazon EC2. If this value is different from `default`, then the instances will be created in EC2 VPC and the parameter `SECURITY_GROUP` will be ignored. For GCE, this parameter is not used.
- `MACHINE_SPEC`: Specifications of the instance to be created. You can specify attributes such as the operating system, Amazon Machine Image (AMI), hardware properties, EC2 instance type and EC2 region. Please see the [Setting Up For EC2 section](#setting-upy-for-ec2) for an example `MACHINE-SPEC` value and please refer to the `TemplateBuilderSpec` class of the `org.jclouds.compute.domain` package at JClouds JavaDoc for a full list of machine specifications.
- `HAZELCAST_VERSION_SPEC`: The workers can be configured to use a specific version of Hazelcast. By this way, you do not need to depend on the Hazelcast version provided by the stabilizer. You can configure the Hazelcast version in one of the following ways:
	- `outofthebox`: This is the default value provided by the Stabilizer itself.
	- `maven=<version>`: Used to give a specific version from the maven repository (for examples, `maven=3.2`, `maven=3.3-SNAPSHOT`). Local Hazelcast artifacts will be preferred, so you can checkout, for example, an experimental branch and build the artifacts locally. This will all be done on the local machine, not on the agent machine.
	- `bringmyown`: Used to specify your own dependencies. For more information on the values, please see the `--workerClassPath` setting of the Controller.
	- `git=version`: if you want Stabilizer to use a specific version of Hazelcast from GIT, e.g., git=f0288f713    to build a specific revision
	git=v3.2.3       to build a version from a GIT tag
	git=myRepository/myBranch - to build a version from a branch in a specific repository.

Use GIT_CUSTOM_REPOSITORIES to specify custom repositories. The main Hazelcast repository is always named "origin".



When HAZELCAST_VERSION_SPEC is set to GIT then it will download Hazelcast sources to this directory.
#
Default value: $HOME/.hazelcast-build/
#
#
GIT_BUILD_DIR=/home/joe/.hazelcast-build/

Comma separated list of additional GIT repositories to be fetched when HAZELCAST_VERSION_SPEC is set to GIT.
#
# Stabilizer will always fetch https://github.com/hazelcast/hazelcast
# This property specifies additional repositories. You can use both remote and local repositories.
# Remote repositories must be accessible for anonymous, local repositories must be accessible for current user.
#
# Default value: empty, only the main Hazelcast repository is used by default.
#
#GIT_CUSTOM_REPOSITORIES=myFork=https://github.com/jerrinot/hazelcast.git,localRepo=/home/jara/devel/oss/hazelcast

# Path to a local Maven installation to use when HAZELCAST_VERSION_SPEC is set to GIT.
#
# Default value: Stabilizer expects 'mvn' binary to be available on a PATH.
#
#MVN_EXECUTABLE=/usr/bin/mvn


# =====================================================================
# JDK Installation
# =====================================================================
#
# Warning:
#   Currently only 64 bit JVM's are going to be installed if you select something else then outofthebox.
#   So make sure that your OS is 64 bits! On option to select 32/64 bits will be added in the future.
#
# The following 4 flavors are available:
#   oracle
#   openjdk
#   ibm
#   outofthebox
# out of the box is the one provided by the image. So no software is installed by the Stabilizer.
#
JDK_FLAVOR=openjdk

#
# If a 64 bits JVM should be installed. Currently only true is allowed.
#
JDK_64_BITS=true

#
# The version of java to install.
#
# Oracle supports 6/7/8
# OpenJDK supports 6/7
# IBM supports 6/7/8 (8 is an early access version)
#
# Fine grained control on the version will be added in the future. Currently it is the most recent released version.
#
JDK_VERSION=7

# =====================================================================
# Profiler configuration
# =====================================================================
#
# Warning: Yourkit only works on 64 bit linux distro's for the time being. No support for windows
# or mac.
#
# The worker can be configured with a profiler. The following options are available:
#   none
#   yourkit
#   hprof
#   perf
#   vtune
#   flightrecorder
#
PROFILER=none


#
# The settings for Oracle Flightrecorder
#
# For options see:
# http://docs.oracle.com/cd/E15289_01/doc.40/e15062/optionxx.htm#BABIECII
#
FLIGHTRECORDER_SETTINGS=-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true


#
# The settings for Yourkit agent.
#
# When Yourkit is enabled, a snapshot is created an put in the worker home directory. So when the artifacts
# are downloaded, the snapshots are included and can be loaded with your Yourkit GUI.
#
# Make sure that the path matches the JVM 32/64 bits. In the future this will be automated.
#
# The libypagent.so files, which are included in Stabilizer, are for "YourKit Java Profiler 2013".
#
# For more information about the Yourkit setting, see:
#   http://www.yourkit.com/docs/java/help/agent.jsp
#   http://www.yourkit.com/docs/java/help/startup_options.jsp
#
YOURKIT_SETTINGS=-agentpath:${STABILIZER_HOME}/yourkit/linux-x86-64/libyjpagent.so=dir=${WORKER_HOME},sampling

#
# The settings for the HProf profiler which is part of the JDK.
#
# By default a 'java.hprof.txt' is created in the worker directory. Which can be downloaded with the
# 'provisioner --download' command after the test has run.
#
# For configuration options see:
#   http://docs.oracle.com/javase/7/docs/technotes/samples/hprof.html
#
HPROF_SETTINGS=-agentlib:hprof=cpu=samples,depth=10

#
# The settings for the 'perf' profiler; available for Linux.
#
# https://perf.wiki.kernel.org/index.php/Tutorial#Sampling_with_perf_record
#
# The settings is the full commandline for 'perf record' excluding the actual arguments for the java program
# to start; these will be provided by the Agent. Once the coordinator completes, all the artifacts (including
# the perf.data created by perf) can be downloaded with 'provisioner --download'. Another option is to log into
# the agent machine and do a 'perf report' locally.
#
# TODO:
# More work needed on documentation to get perf running correctly.
#
# If you get the following message:
#           Kernel address maps (/proc/{kallsyms,modules}) were restricted.
#           Check /proc/sys/kernel/kptr_restrict before running 'perf record'.
# Apply the following under root:
#           echo 0 > /proc/sys/kernel/kptr_restrict
# To make it permanent, add it to /etc/rc.local
#
# If you get the following message while doing call graph analysis (-g)
#            No permission to collect stats.
#            Consider tweaking /proc/sys/kernel/perf_event_paranoid.
# Apply the following under root:
#           echo -1 > /proc/sys/kernel/perf_event_paranoid
# To make it permanent, add it to /etc/rc.local
#
PERF_SETTINGS=perf record -o perf.data --quiet
#PERF_SETTINGS=perf record -o perf.data --quiet -e cpu-cycles -e cpu-clock -e context-switches


#
# The settings for the 'Intel VTune' profiler.
#
# It requires Intel VTune to be installed on the system.
#
# The settings is the full commandline for the amplxe-cl excluding the actual arguments for the java program
# to start; these will be provided by the Agent. Once the coordinator completes, all the artifacts can be downloaded with
# 'provisioner --download'.
#
# To see within the JVM, make sure that you locally have the same Java version (under the same path) as the stabilizer. Else
# VTune will not be able to see within the JVM.
#
# Reference to amplxe-cl commandline options:
# https://software.intel.com/sites/products/documentation/doclib/iss/2013/amplifier/lin/ug_docs/GUID-09766DB6-3FA8-445B-8E70-5BC9A1BE7C55.htm#GUID-09766DB6-3FA8-445B-8E70-5BC9A1BE7C55
#
VTUNE_SETTINGS=/opt/intel/vtune_amplifier_xe/bin64/amplxe-cl -collect hotspots




