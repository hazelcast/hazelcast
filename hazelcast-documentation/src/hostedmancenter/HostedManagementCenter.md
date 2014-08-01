


---



# Introduction

Hazelcast Hosted Management Center enables you to monitor and manage your nodes running Hazelcast. In addition to monitoring overall state of your clusters, you can also analyze and browse your data structures in detail, update map configurations and take thread dump from nodes. With its scripting and console module, you can run scripts (JavaScript, Groovy, etc.) and commands on your nodes. It is a web based tool and you can deploy it into your internal server and serve your users. ^super^script 

To be able to use Hosted Management Center to monitor your cluster:

1- You need to configure your cluster so it can be connected from this tool.

2- You need to sign up on the tool to create an account.

---

# Configuration
In order to monitor your cluster in Hosted Management Center, `hazelcast.hosted.management.enabled` system property needs to be set as `true` in your cluster. This can be done with one of the ways listed below:

1- Using JVM command line argument `-Dhazelcast.hosted.management.enabled=true`

or
 
2- Adding the snippet below to your `hazelcast.xml` configuration file. 


```xml
<properties>
     <property name="hazelcast.hosted.management.enabled">true</property>
</properties>
```


Also, `security-token` property in the management center configuration should be specified. This token can be grabbed from Hosted Management Center after registration (please see next section). It can be added to `hazelcast.xml` configuration file with below line.

```xml
<management-center security-token="YOUR_SECURITY_TOKEN" />
```

---

# Sign Up & Login
Go to [http://manage.hazelcast.com/3.2/](http://manage.hazelcast.com/3.2/) using any web browser. Below page will load.

![](images/1Login.jpg)

Click on **Sign Up** link and fill the form that will appear and ask your name, company, e-mail and password. Once you click on the **Sign Up** button on this form, you will be directed to the page where cluster configuration is explained and security token is provided, as shown below.

![](images/RegistrationWindow.jpg)

Add this token (value of the *security-token*) to your `hazelcast.xml` configuration file as explained in [Configuration](#configuration) section above. Click on **Back to Dashboard** button and login page loads.

Once the credentials are entered and **Login** key is pressed, the tool will ask from the user to choose a cluster, as shown below.

![](images/2SelectCluster.jpg)

Select the cluster and hit **Connect** button.

If the tool shows a warning as shown below, check your cluster's configuration and be sure you performed the steps explained in [Configuration](#configuration) section above.


---

# Tool Overview

Once the page is loaded after selecting a cluster, tool's home page appears as shown below.

![](images/3HomePage.jpg)

This page provides the fundamental properties of the selected cluster which are explained in [Home Page](#homepage).

It also has a toolbar on the top and a menu on the left.

## Toolbar
Toolbar has the following buttons:

-	**Home**: When pressed, loads the home page shown above. Please see [Home Page](#homepage).
-	**Scripting**: When pressed, loads the page used to write and execute user`s own scripts on the cluster. Please see [Scripting](#scripting).
-	**Console**: When pressed, loads the page used to execute commands on the cluster. Please see [Console](#console).
-	**Documentation**: It is used to open the documentation of Hosted Management Center in a window inside the tool. Please see [Documentation](#documentation).
-	**Cluster Selector**: It is used to switch between clusters. When the mouse is moved onto this item, a dropdown list of clusters appears.

     ![](images/4ChangeCluster.jpg)
     
     The user can select any cluster and once selected, the page immediately loads with the selected cluster's information.
-	**Logout**: It is used to close the current user's session.

## Menu
Home page includes a menu on the left which lists the distributed data structures in the cluster and also all cluster members (nodes), as shown below.

![](images/LeftMenu.jpg)

Menu items can be expanded/collapsed by clicking on them. Below is the list of menu items with the links to their explanations.
     
-	[Maps](#maps)
-	[Queues](#queues)
-	[Topics](#topics)
-	[MultiMaps](#MultiMaps)
-	[Executors](#executors)
-	[Members](#members)

## Tabbed View
Each time an item from the toolbar or menu is selected, it is added to main view as a tab, as shown below.

![](images/TabbedView.jpg)

In the above example, *Home*, *Scripting*, *Console*, *queue1* and *map1* windows can be seen as tabs. Windows can be closed using the ![](images/CloseIcon.jpg) icon on each tab (except the Home Page; it cannot be closed).

---


# Home Page
This is the first page appearing after logging in. It gives an overview of the cluster connected. Below subsections describe each portion of the page.

## Cluster Info
On top of the page, **Cluster Info** is placed, as shown below.

![](images/HomeClusterInfo.jpg)

-	**Version**: Shows the release number of Hazelcast used.
-	**Start Time**: It is the date and time (UTC) when first node of the connected cluster was started.
-	**Up Time**: Shows the period of time in days, hours, minutes and seconds for which connected Hazelcast cluster is up.
-	**Used Memory**: Shows the real-time total memory used in the whole cluster.
-	**Free Memory**: Shows the real-time memory that is free in the whole cluster.
-	**Max Memory**: Shows the maximum memory that the whole cluster has.

## CPU Load Averages & Utilization
This part of the page provides information related to load and utilization of CPUs for each node, as shown below.

![](images/Home-CPULoadAveragesUtilization.jpg)

First column lists the nodes with their IPs and ports. Then, the loads on each CPU for the last 1, 5 and 15 minutes are listed. The last column (**Utilization(%)**) shows the utilization of CPUs graphically. When you move the mouse cursor on a desired graph, you can see the CPU utilization at the time to which cursor corresponds. In the above example, the CPU utilization at 10:53:25 (where the mouse cursor is on) is -1%. Graphs under this column shows the CPU utilizations approximately for the last 2 minutes.


## Memory Usages
This part of the page provides information related to memory usages for each node, as shown below.

![](images/Home-MemoryUsages.jpg)

First column lists the nodes with their IPs and ports. Then, used and free memories out of the total memory reserved for Hazelcast usage are shown, in real-time. **Max** column lists the maximum memory capacity of each node and **Percent** column lists the percentage value of used memory out of the maximum memory. The last column (**Used Memory(%)**) shows the memory usage of nodes graphically. When you move the mouse cursor on a desired graph, you can see the memory usage at the time to which cursor corresponds. In the above example, the memory usage at 10:53:55 (where the mouse cursor is on) is 32 MB. Graphs under this column shows the memory usages approximately for the last 2 minutes.

## Memory Distribution
This part of the page graphically provides the cluster wise breakdown of memory, as shown below. Blue area is the memory used by maps, dark yellow area is the memory used by non-Hazelcast entities and green area is the free memory (out of whole cluster`s memory capacity).

![](images/Home-MemoryDistribution.jpg)

In the above example, you can see 0.32% of the total memory is used by Hazelcast maps (it can be seen by moving the mouse cursor on it), 58.75% is used by non-Hazelcast entities and 40.85% of the total memory is free.

## Map Memory Distribution
This part is actually the breakdown of the blue area shown in **Memory Distribution** graph explained above. It provides the percentage values of the memories used by each map, out of the total cluster memory reserved for all Hazelcast maps.

![](images/Home-MapMemoryDistribution.jpg)

In the above example, you can see 49.55% of the total map memory is used by **map1** and 49.55% is used by **map2**.

## Health Check
This part is useful to check how the cluster in general behaves. It lists the nodes (cluster members), locks and partition mismatches along with the information related to migrations and node interconnections. To see these, just click on **Check Cluster Health** button. A sample is shown below.

![](images/Home-HealthCheckbuttonpressed.jpg)

You can see each node's IP address and port by clicking on the plus sign at the **Members**.

## Partition Distribution
This pie chart shows what percentage of partitions each node has, as shown below.

![](images/Home-PartitionDistribution.jpg)

You can see each node's partition percentages by moving the mouse cursor on the chart. In the above example, you can see the node "127.0.0.1:5708" has 5.64% of the total partition count (which is 271 by default and configurable, please see [Advanced Configuration Properties](http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#advanced-configuration-properties)).

## System Warnings
This part of the page shows informative warnings in situations like shutting down a node, as shown below.

![](images/SystemWarnings.jpg)

Warnings can be cleared by clicking on the **Clear** link placed at top right of the window.

---


# Maps

Map instances are listed under the **Maps** menu item on the left. When you click on a map, a new tab for monitoring that map instance is opened on the right, as shown below. In this tab, you can monitor metrics and also re-configure the selected map.

![](images/MapsHome.jpg)

Below subsections explain the portions of this window.

## Map Browser

Map Browser is a tool used to retrieve properties of the entries stored in the selected map. It can be opened by clicking on the **Map Browser** button, located at top right of the window. Once opened, the tool appears as a dialog, as shown below.

![](images/Map-MapBrowser.jpg)

Once the key and key's type is specified and **Browse** button is clicked, key's properties along with its value is listed.

## Map Config
By using Map Config tool, you can set selected map's attributes like the backup count, TTL, and eviction policy. It can be opened by clicking on the **Map Config** button, located at top right of the window. Once opened, the tool appears as a dialog, as shown below.

![](images/Map-MapConfig.jpg)

Change any attribute as required and click **Update** button to save changes.


## Map Monitoring

Besides Map Browser and Map Config tools, this page has many  monitoring options explained below. All of these perform real-time monitoring. 

On top of the page, there are small charts to monitor the size, throughput, memory usage, backup size, etc. of the selected map in real-time. All charts' X-axis shows the current system time. Other small monitoring charts can be selected using ![](images/ChangeWindowIcon.jpg) button placed at top right of each chart. When it is clicked, the whole list of monitoring options are listed, as shown below.

![](images/SelectConfOpt.jpg)

When you click on a desired monitoring, the chart is loaded with the selected option. Also, a chart can be opened as a separate dialog by clicking on the ![](images/MaximizeChart.jpg) button placed at top right of each chart. Below monitoring charts are available:

-	**Size**: Monitors the size of the map. Y-axis is the entry count (should be multiplied by 1000).
-	**Throughput**: Monitors get, put and remove operations performed on the map. Y-axis is the operation count.
-	**Memory**: Monitors the memory usage on the map. Y-axis is the memory count.
-	**Backups**: It is the chart loaded when "Backup Size" is selected. Monitors the size of the backups in the map. Y-axis is the backup entry count (should be multiplied by 1000).
-	**Backup Memory**: It is the chart loaded when "Backup Mem." is selected. Monitors the memory usage of the backups. Y-axis is the memory count.
-	**Hits**: Monitors the hit count of the map.
-	**Puts/s, Gets/s, Removes/s**: These three charts monitor the put, get and remove operations (per second) performed on the selected map.


Under these charts, there are **Map Memory** and **Map Throughput** data tables. Map Memory data table provides memory metrics distributed over nodes, as shown below.

![](images/Map-MemoryDataTable.jpg)

From left to right, this table lists the IP address and port, entry counts, memory used by entries, backup entry counts, memory used by backup entries, events, hits, locks and dirty entries (in the cases where *MapStore* is enabled, these are the entries that are put to/removed from the map but not written to/removed from a database yet) of each node in the map. You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.

Map Throughput data table provides information about the operations (get, put, remove) performed on each node in the map, as shown below.

![](images/Map-MapThroughputDataTable.jpg)

From left to right, this table lists the IP address and port of each node, put, get and remove operations on each node, average put, get, remove latencies and maximum put, get, remove latencies on each node. 

You can select the period in the combo box placed at top right corner of the window, for which the table data will be shown. Available values are **Since Beginning**, **Last Minute**, **Last 10 Minutes** and **Last 1 Hour**. 

You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.


---

# Queues

Using the menu item **Queues**, you can monitor your queues data structure. When you expand this menu item and click on a queue, a new tab for monitoring that queue instance is opened on the right, as shown below.

![](images/Queues-Home.jpg)


On top of the page, there are small charts to monitor the size, offers and polls of the selected queue in real-time. All charts' X-axis shows the current system time. And a chart can be opened as a separate dialog by clicking on the ![](images/MaximizeChart.jpg) button placed at top right of each chart. Below monitoring charts are available:

-	**Size**: Monitors the size of the queue. Y-axis is the entry count (should be multiplied by 1000).
-	**Offers**: Monitors the offers sent to the selected queue. Y-axis is the offer count.
-	**Polls**: Monitors the polls sent to the selected queue. Y-axis is the poll count.


Under these charts, there are **Queue Statistics** and **Queue Operation Statistics** tables. Queue Statistics table provides item and backup item counts in the queue and age statistics of items and backup items at each node, as shown below.

![](images/QueueStatistics.jpg)

From left to right, this table lists the IP address and port, items and backup items on the queue of each node, and maximum, minimum and average age of items in the queue. You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.

Queue Operations Statistics table provides information about the operations (offers, polls, events) performed on the queues, as shown below.

![](images/QueueOperationStatistics.jpg)

From left to right, this table lists the IP address and port of each node, and counts of offers, rejected offers, polls, poll misses and events. 

You can select the period in the combo box placed at top right corner of the window, for which the table data will be shown. Available values are **Since Beginning**, **Last Minute**, **Last 10 Minutes** and **Last 1 Hour**. 

You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.

---

# Topics

You can monitor your topics' metrics by clicking the topic name listed on the left panel under **Topics** menu item. A new tab for monitoring that topic instance is opened on the right, as shown below.

![](images/ManCenter-Topics.jpg)

On top of the page, there are two charts to monitor the **Publishes** and **Receives** in real-time. They show the published and received message counts of the cluster, nodes of which are subscribed to the selected topic. Both charts' X-axis shows the current system time. and a chart can be opened as a separate dialog by clicking on the ![](images/MaximizeChart.jpg) button placed at top right of each chart.

Under these charts, there is Topic Operation Statistics table. From left to right, this table lists the IP addresses and ports of each node, and counts of message published and receives per second in real-time. You can select the period in the combo box placed at top right corner of the table, for which the table data will be shown. Available values are **Since Beginning**, **Last Minute**, **Last 10 Minutes** and **Last 1 Hour**. 

You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.



---

# MultiMaps
This monitoring option is similar to the **Maps** one. Same monitoring charts and data tables are used to monitor MultiMaps. Differences are; not being able to browse the MultiMaps and to re-configure it. Please see [Maps](#maps).

---

# Executors
Executor instances are listed under the **Executors** menu item on the left. When you click on a executor, a new tab for monitoring that executor instance is opened on the right, as shown below.

![](images/ExecutorsHome.jpg)

On top of the page, there are small charts to monitor the pending, started, completed, etc. executors in real-time. All charts' X-axis shows the current system time. Other small monitoring charts can be selected using ![](images/ChangeWindowIcon.jpg) button placed at top right of each chart. When it is clicked, the whole list of monitoring options are listed, as shown below.

![](images/SelectExecMonOpt.jpg)

When you click on a desired monitoring, the chart is loaded with the selected option. Also, a chart can be opened as a separate dialog by clicking on the ![](images/MaximizeChart.jpg) button placed at top right of each chart. Below monitoring charts are available:

-	**Pending**: Monitors the pending executors. Y-axis is the executor count.
-	**Started**: Monitors the started executors. Y-axis is the executor count.
-	**Start Lat. (msec)**: Shows the latency when executors are started. Y-axis is the duration in milliseconds.
-	**Completed**: Monitors the completed executors. Y-axis is the executor count.
-	**Comp. Time (msec)**: Shows the completion period of executors. Y-axis is the duration in milliseconds.

Under these charts, there is **Executor Operation Statistics** table, as shown below.

![](images/ExecutorOperationStats.jpg)

From left to right, this table lists the IP address and port of nodes, counts of pending, started and completed executors per second, execution time and average start latency of executors on each node. You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.

---


# Members

This menu item is used to monitor each cluster member (node) and also perform operations like running garbage colletion (GC) and taking a thread dump. Once a member is selected form the menu, a new tab for monitoring that member is opened on the right, as shown below.

![](images/MembersHome.jpg)

**CPU Utilization** chart shows the CPU usage on the selected member in percentage. **Memory Utilization** chart shows the memory usage on the selected member with three different metrics (maximum, used and total memory). Both of these charts can be opened as separate windows using the ![](images/ChangeWindowIcon.jpg) button placed at top right of each chart, a more clearer view can be obtained by this way.

The window titled with **Partitions** shows which partitions are assigned to the selected member. **Runtime** is a dynamically updated window tab showing the processor number, start and up times, maximum, total and free memory sizes of the selected member. Next to this, there is **Properties** tab showing the system properties. **Member Configuration** window shows the connected Hazelcast cluster's XML configuration.

Besides the aforementioned monitoring charts and windows, there are also operations you can perform on the selected memberthrough this page. You can see operation buttons located at top right of the page, explained below:

-	**Run GC**: When pressed, garbage collection is executed on the selected member. A notification stating that the GC execution was successful will be shown.
-	**Thread Dump**: When pressed, thread dump of the selected member is taken and shown as a separate dialog to th user.
-	**Shutdown Node**: It is used to shutdown the selected member.

---

# Scripting

Scripting feature of this tool is used to execute codes on the cluster. You can open this feature as a tab by selecting **Scripting** located at the toolbar on top. Once selected, it is opened as shown below.

![](images/Scripting.jpg)

In this window, **Scripting** part is the actual coding editor. You can select the members on which the code will be executed from the **Members** list shown at the right side of the window. Below the members list there is a combo box enabling you to select a scripting language. Currently, Javascript, Ruby, Groovy and Python languages are supported. After you write your script and press **Execute** button, you can see the execution result in the **Result** part of the window. 

There are also **Save** and **Delete** buttons on top right of the scripting editor. You can save your scripts by pressing the **Save** button after you type a name for the script into the field next to this button. The scripts you saved are listed in the **Saved Scripts** part of the window, located at the bottom right of the page. You can simply click on a saved script from this list to execute or edit it. And, if you want to remove a script that you wrote and save before, just select it from this list and press **Delete** button.

---

# Console

Hosted Management Center has also a console feature that enables you execute commands on the cluster. For example, you can perform "put"s and "get"s on a map, after you set the namespace with the command `ns <name of your map>`. Same is valid for queues, topics, etc. To execute your command, just type it into the field below the console and press **Enter**. You can type `help` to see all commands that can be used. 

Console window can be opened by clicking on the **Console** button located at the toolbar. A sample view with some commands executed can ben seen below.

![](images/Console.jpg)

---

# Documentation
To see the documentation, click on the **Documentation** button located at the toolbar. This document will appear as a tab.


