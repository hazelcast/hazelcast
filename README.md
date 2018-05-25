# hazelcast-gcp
Google Cloud Platform

PRD: https://hazelcast.atlassian.net/wiki/spaces/PM/pages/91259071/Google+Cloud+Platform+Integration


Requirements
Hazelcast 3.6+
Linux Kernel 3.19+ (TCP connections may get stuck when used with older Kernel versions, resulting in undefined timeouts)
Discovering Members within Google Cloud Platform
Hazelcast supports GCP auto-discovery. It is useful when you do not want to provide or you cannot provide the list of possible IP addresses.

There are two possible ways to configure your cluster to use GCP auto-discovery. You can either choose to configure your cluster with GcpConfig (or gcp element in your XML config) or you can choose configuring GCP discovery using Discovery SPI implementation.

Zone Aware Support
NOTE: ZONE_AWARE configuration is only valid when you use Hazelcast Discovery SPI based configuration with <discovery-strategies>. 

Zone Aware Support is available for Hazelcast Client 3.8.6 and newer releases.

As a discovery service, Hazelcast GCP plugin put the zone information into the Hazelcast's member attributes map during the discovery process. Please see the Defining Member Attributes section to learn about the member attributes.

When using ZONE_AWARE configuration, backups are created in the other AZs. Each zone will be accepted as one partition group.

