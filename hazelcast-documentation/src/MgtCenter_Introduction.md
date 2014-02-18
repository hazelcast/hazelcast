
## Introduction

Hazelcast Management Center enables you to monitor and manage your servers running hazelcast. With Management Center, in addition to monitoring overall state of your clusters, you can also analyze and browse your data structures in details. You can also update map configurations and take thread dump from nodes. With its scripting module, you can run scritps (JavaScript, Groovy etc.) on your servers. Version 2.0 is a web based tool so you can deploy it into your internal server and serve your users.

### Installation

It is important to understand how it actually works. Basically you will deploy `mancenter`-*version*`.war` application into your Java web server and then tell Hazelcast nodes to talk to that web application. That means, your Hazelcast nodes should know the URL of `mancenter` application before they start.

Here are the steps:

-   Download the latest Hazelcast zip from [hazelcast.org](http://www.hazelcast.org/download/)

-   Zip contains `mancenter`-*version*`.war` file. Deploy it to your web server (Tomcat, Jetty etc.) Let's say it is running at`http://localhost:8080/mancenter-`*version*

-   Start your web server and make sure `http://localhost:8080/mancenter`-*version*` is up.

-   Configure your Hazelcast nodes by adding the URL of your web app to your `hazelcast.xml`. Hazelcast nodes will send their states to this URL.

```xml
<management-center enabled="true">http://localhost:8080/mancenter-```*version*```</management-center>
```
-   Start your Hazelcast cluster.

-   Browse to `http://localhost:8080/mancenter`-*version* and login. **Initial login username/passwords is `admin/admin`**

*Management Center creates a directory with name "mancenter" under your "user/home" directory to save data files. You can change the data directory setting "hazelcast.mancenter.home" system property.*

### User Administration

Default credentials are for the admin user. In the `Administration` tab, Admin can add/remove/update users and control user read/write permissions.

![](images/admin.jpg)

### Tool Overview

The starter page of the tool is`Cluster Home`. Here you can see cluster's main properties such as uptime, memory. Also with pie chart, you can see the distribution of partitions over cluster members. You can come back to this page, by clicking the `Home` icon on the top-right toolbar. On the left panel you see the Map/Queue/Topic instances in the cluster. At the bottom-left corner, members of the cluster are listed. On top menu bar, you can change the current tab to`Scripting, Docs`, user`Administration`. Note that Administration tab is viewable only for admin users. Also `Scripting` page is disabled for users with read-only credential.

![](images/clusterhome.jpg)
