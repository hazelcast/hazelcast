


## Management Center


### Introduction

Hazelcast Management Center enables you to monitor and manage your nodes running Hazelcast. In addition to monitoring overall state of your clusters, you can also analyze and browse your data structures in detail, update map configurations and take thread dump from nodes. With its scripting and console module, you can run scripts (JavaScript, Groovy, etc.) and commands on your nodes.

#### Installation

You have two options for installing Hazelcast Management Center. You can either deploy the `mancenter`-*version*`.war` application into your Java application server/container or you can start Hazelcast Management Center directly from the command line and then have the Hazelcast nodes communicate with that web application. This means that your Hazelcast nodes should know the URL of the `mancenter` application before they start.

Here are the steps:

-   Download the latest Hazelcast ZIP from [hazelcast.org](http://www.hazelcast.org/download/). The ZIP contains the `mancenter`-*version*`.war` file. 
-   You can directly start `mancenter`-*version*`.war` file from the command line. The following command will start Hazelcast Management Center on port 8080 with context root 'mancenter' (`http://localhost:8080/mancenter`).

```java
java -jar mancenter-*version*.war 8080 mancenter
```

- Or, you can deploy it to your web server (Tomcat, Jetty, etc.). Let us say it is running at `http://localhost:8080/mancenter`.
- After you perform the above steps, make sure that `http://localhost:8080/mancenter` is up.
- Configure your Hazelcast nodes by adding the URL of your web application to your `hazelcast.xml`. Hazelcast nodes will send their states to this URL.


```xml
<management-center enabled="true">http://localhost:8080/mancenter</management-center>
```

-   Start your Hazelcast cluster.

-   Browse to `http://localhost:8080/mancenter` and login. **Initial login username/password is `admin/admin`**

The Management Center creates a directory with the name "mancenter" under your "user/home" directory to save data files. You can change the data directory by setting the `hazelcast.mancenter.home` system property.



### Tool Overview

Once the page is loaded after selecting a cluster, the tool's home page appears as shown below.

![](images/NonHostedMCHomePage.jpg)

This page provides the fundamental properties of the selected cluster which are explained in the [Home Page](#homepage) section. The page has a toolbar on the top and a menu on the left.

#### Toolbar
The toolbar has the following buttons:

-	**Home**: When pressed, loads the home page shown above. Please see [Home Page](#homepage).
-	**Scripting**: When pressed, loads the page used to write and execute user`s own scripts on the cluster. Please see [Scripting](#scripting).
-	**Console**: When pressed, loads the page used to execute commands on the cluster. Please see [Console](#console).
-	**Alerts**: Creates alerts by specifying filters. Please see [Alerts](#alerts).
-	**Documentation**: Opens the Management Center documentation in a window inside the tool. Please see [Documentation](#documentation).
-	**Administration**: Used by the admin users to manage users in the system. Please see [Administration](#administration).
-	**Time Travel**: Used to see the cluster's situation at a time in the past. Please see [Time Travel](#time-travel).
-	**Cluster Selector**: Used to switch between clusters. When the mouse is moved onto this item, a drop down list of clusters appears.

  ![](images/4ChangeCluster.jpg)

  The user can select any cluster and once selected, the page immediately loads with the selected cluster's information.
-	**Logout**: Closes the current user's session.


![image](images/NoteSmall.jpg) ***NOTE:*** *Some of the above listed toolbar items are not visible to users who are not admin or who have **read-only** permission. Also, some of the operations explained in the later sections cannot be performed by users with read-only permission. Please see [Administration](#administration) for details.*


#### Menu
The Home page includes a menu on the left which lists the distributed data structures in the cluster and all the cluster members (nodes), as shown below.

![](images/LeftMenu.jpg)

![image](images/NoteSmall.jpg) ***NOTE:*** *Distributed data structures will be shown there when the proxies are created for them.*


You can expand and collapse menu items by clicking on them. Below is the list of menu items with links to their explanations.

- [Caches](#caches)
- [Maps](#maps)
-	[Queues](#queues)
-	[Topics](#topics)
-	[MultiMaps](#MultiMaps)
-	[Executors](#executors)
-	[Members](#members)

#### Tabbed View
Each time you select an item from the toolbar or menu, the item is added to the main view as a tab, as shown below.

![](images/NonHMCTabbedView.jpg)

In the above example, *Home*, *Scripting*, *Console*, *queue1* and *map1* windows can be seen as tabs. Windows can be closed using the ![](images/CloseIcon.jpg) icon on each tab (except the Home Page; it cannot be closed).

---


### Home Page
This is the first page appearing after logging in. It gives an overview of the connected cluster. The following subsections describe each portion of the page.


#### CPU Utilization
This part of the page provides load and utilization information for the CPUs for each node, as shown below.

![](images/NonHMCCPUUtil.jpg)

The first column lists the nodes with their IPs and ports. The next columns list the loads on each CPU for the last 1, 5 and 15 minutes. The last column (**Chart**) graphically shows the utilization of CPUs. When you move the mouse cursor on a desired graph, you can see the CPU utilization at the time where the cursor is placed. Graphs under this column shows the CPU utilizations approximately for the last 2 minutes.


#### Memory Utilization
This part of the page provides information related to memory usages for each node, as shown below.

![](images/NonHMCMemoryUtil.jpg)

The first column lists the nodes with their IPs and ports. The next columns show the used and free memories out of the total memory reserved for Hazelcast usage, in real-time. The **Max** column lists the maximum memory capacity of each node and the **Percent** column lists the percentage value of used memory out of the maximum memory. The last column (**Chart**) shows the memory usage of nodes graphically. When you move the mouse cursor on a desired graph, you can see the memory usage at the time where the cursor is placed. Graphs under this column shows the memory usages approximately for the last 2 minutes.

#### Memory Distribution
This part of the page graphically provides the cluster wise breakdown of memory, as shown below. The blue area is the memory used by maps, the dark yellow area is the memory used by non-Hazelcast entities, and the green area is the free memory out of the whole cluster`s memory capacity.

![](images/Home-MemoryDistribution.jpg)

In the above example, you can see 0.32% of the total memory is used by Hazelcast maps (it can be seen by placing the mouse cursor on it), 58.75% is used by non-Hazelcast entities and 40.85% of the total memory is free.

#### Map Memory Distribution
This part is the breakdown of the blue area shown in the **Memory Distribution** graph explained above. It provides the percentage values of the memories used by each map, out of the total cluster memory reserved for all Hazelcast maps.

![](images/Home-MapMemoryDistribution.jpg)

In the above example, you can see 49.55% of the total map memory is used by **map1** and 49.55% is used by **map2**.

#### Partition Distribution
This pie chart shows what percentage of partitions each node has, as shown below.

![](images/Home-PartitionDistribution.jpg)

You can see each node's partition percentages by placing the mouse cursor on the chart. In the above example, you can see the node "127.0.0.1:5708" has 5.64% of the total partition count (which is 271 by default and configurable, please see [Advanced Configuration Properties](http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#advanced-configuration-properties)).

---
### Caches

You can monitor your caches' metrics by clicking the cache name listed on the left panel under **Caches** menu item. A new tab for monitoring that cache instance is opened on the right, as shown below.

![](images/ManCenter-Caches.jpg)

On top of the page, four charts monitor the **Gets**, **Puts**, **Removals** and **Evictions** in real-time. The X-axis of all the charts show the current system time. To open a chart as a separate dialog, click on the ![](images/MaximizeChart.jpg) button placed at the top right of each chart.

Under these charts is the Cache Statistics Data Table. From left to right, this table lists the IP addresses and ports of each node, get, put, removal, eviction, hit and miss count per second in real-time.

You can navigate through the pages using the buttons at the bottom right of the table (**First, Previous, Next, Last**). You can ascend or descend the order of the listings in each column by clicking on column headings.

### Maps

Map instances are listed under the **Maps** menu item on the left. When you click on a map, a new tab for monitoring that map instance opens on the right, as shown below. In this tab, you can monitor metrics and also re-configure the selected map.

![](images/MapsHome.jpg)

The below subsections explain the portions of this window.

#### Map Browser

Map Browser is a tool you can use to retrieve properties of the entries stored in the selected map. To open it, click on the **Map Browser** button, located at the top right of the window. Once opened, the tool appears as a dialog, as shown below.

![](images/Map-MapBrowser.jpg)

Once the key and key's type is specified and the **Browse** button is clicked, the key's properties along with its value are listed.

#### Map Config
By using the Map Config tool, you can set selected map's attributes like the backup count, TTL, and eviction policy. To open it, click on the **Map Config** button, located at the top right of the window. Once opened, the tool appears as a dialog, as shown below.

![](images/Map-MapConfig.jpg)

Change any attribute as required and click the **Update** button to save changes.


#### Map Monitoring

Besides Map Browser and Map Config tools, this page has monitoring options explained below. All of these options perform real-time monitoring.

On top of the page, small charts monitor the size, throughput, memory usage, backup size, etc. of the selected map in real-time. The X-axis of all the charts show the current system time. You can select other small monitoring charts using the ![](images/ChangeWindowIcon.jpg) button at the top right of each chart. When you click the button, the monitoring options are listed, as shown below.

![](images/SelectConfOpt.jpg)

When you click on a desired monitoring, the chart is loaded with the selected option. To open a chart as a separate dialog, click on the ![](images/MaximizeChart.jpg) button placed at the top right of each chart. The monitoring charts below are available:

-	**Size**: Monitors the size of the map. Y-axis is the entry count (should be multiplied by 1000).
-	**Throughput**: Monitors get, put and remove operations performed on the map. Y-axis is the operation count.
-	**Memory**: Monitors the memory usage on the map. Y-axis is the memory count.
-	**Backups**: It is the chart loaded when "Backup Size" is selected. Monitors the size of the backups in the map. Y-axis is the backup entry count (should be multiplied by 1000).
-	**Backup Memory**: It is the chart loaded when "Backup Mem." is selected. Monitors the memory usage of the backups. Y-axis is the memory count.
-	**Hits**: Monitors the hit count of the map.
-	**Puts/s, Gets/s, Removes/s**: These three charts monitor the put, get and remove operations (per second) performed on the selected map.


Under these charts are **Map Memory** and **Map Throughput** data tables. The Map Memory data table provides memory metrics distributed over nodes, as shown below.

![](images/Map-MemoryDataTable.jpg)

From left to right, this table lists the IP address and port, entry counts, memory used by entries, backup entry counts, memory used by backup entries, events, hits, locks and dirty entries (in the cases where *MapStore* is enabled, these are the entries that are put to/removed from the map but not written to/removed from a database yet) of each node in the map. You can navigate through the pages using the buttons at the bottom right of the table (**First, Previous, Next, Last**). You can ascend or descend the order of the listings by clicking on the column headings.

Map Throughput data table provides information about the operations (get, put, remove) performed on each node in the map, as shown below.

![](images/Map-MapThroughputDataTable.jpg)

From left to right, this table lists the IP address and port of each node, the put, get and remove operations on each node, the average put, get, remove latencies, and the maximum put, get, remove latencies on each node.

You can select the period in the combo box placed at the top right corner of the window, for which the table data will be shown. Available values are **Since Beginning**, **Last Minute**, **Last 10 Minutes** and **Last 1 Hour**.

You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). To ascend or descent the order of the listings, click on the column headings.

### Queues

Using the menu item **Queues**, you can monitor your queues data structure. When you expand this menu item and click on a queue, a new tab for monitoring that queue instance is opened on the right, as shown below.

![](images/Queues-Home.jpg)


On top of the page, small charts monitor the size, offers and polls of the selected queue in real-time. The X-axis of all the charts shows the current system time. To open a chart as a separate dialog, click on the ![](images/MaximizeChart.jpg) button placed at the top right of each chart. The monitoring charts below are available:

-	**Size**: Monitors the size of the queue. Y-axis is the entry count (should be multiplied by 1000).
-	**Offers**: Monitors the offers sent to the selected queue. Y-axis is the offer count.
-	**Polls**: Monitors the polls sent to the selected queue. Y-axis is the poll count.


Under these charts are **Queue Statistics** and **Queue Operation Statistics** tables. The Queue Statistics table provides item and backup item counts in the queue and age statistics of items and backup items at each node, as shown below.

![](images/QueueStatistics.jpg)

From left to right, this table lists the IP address and port, items and backup items on the queue of each node, and maximum, minimum and average age of items in the queue. You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). The order of the listings in each column can be ascended or descended by clicking on column headings.

Queue Operations Statistics table provides information about the operations (offers, polls, events) performed on the queues, as shown below.

![](images/QueueOperationStatistics.jpg)

From left to right, this table lists the IP address and port of each node, and counts of offers, rejected offers, polls, poll misses and events.

You can select the period in the combo box placed at the top right corner of the window to show the table data. Available values are **Since Beginning**, **Last Minute**, **Last 10 Minutes** and **Last 1 Hour**.

You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). Click on the column headings to ascend or descend the order of the listings.

### Topics

To monitor your topics' metrics, click the topic name listed on the left panel under the **Topics** menu item. A new tab for monitoring that topic instance opens on the right, as shown below.

![](images/ManCenter-Topics.jpg)

On top of the page, two charts monitor the **Publishes** and **Receives** in real-time. They show the published and received message counts of the cluster, nodes of which are subscribed to the selected topic. The X-axis of both charts show the current system time. To open a chart as a separate dialog, click on the ![](images/MaximizeChart.jpg) button placed at the top right of each chart.

Under these charts is the Topic Operation Statistics table. From left to right, this table lists the IP addresses and ports of each node, and counts of message published and receives per second in real-time. You can select the period in the combo box placed at top right corner of the table to show the table data. The available values are **Since Beginning**, **Last Minute**, **Last 10 Minutes** and **Last 1 Hour**.

You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). Click on the column heading to ascend or descend the order of the listings.


### MultiMaps
MultiMap is a specialized map where you can associate a key with multiple values. This monitoring option is similar to the **Maps** option: the same monitoring charts and data tables monitor MultiMaps. The differences are that you cannot browse the MultiMaps and re-configure it. Please see [Maps](#maps).



### Executors
Executor instances are listed under the **Executors** menu item on the left. When you click on a executor, a new tab for monitoring that executor instance opens on the right, as shown below.

![](images/ExecutorsHome.jpg)

On top of the page, small charts monitor the pending, started, completed, etc. executors in real-time. The X-axis of all the charts shows the current system time. You can select other small monitoring charts using the ![](images/ChangeWindowIcon.jpg) button placed at the top right of each chart. When it is clicked, the monitoring options are listed, as shown below.

![](images/SelectExecMonOpt.jpg)

When you click on a desired monitoring, the chart loads with the selected option. To open a chart as a separate dialog, click on the ![](images/MaximizeChart.jpg) button placed at top right of each chart. The below monitoring charts are available:

-	**Pending**: Monitors the pending executors. Y-axis is the executor count.
-	**Started**: Monitors the started executors. Y-axis is the executor count.
-	**Start Lat. (msec)**: Shows the latency when executors are started. Y-axis is the duration in milliseconds.
-	**Completed**: Monitors the completed executors. Y-axis is the executor count.
-	**Comp. Time (msec)**: Shows the completion period of executors. Y-axis is the duration in milliseconds.

Under these charts is the **Executor Operation Statistics** table, as shown below.

![](images/ExecutorOperationStats.jpg)

From left to right, this table lists the IP address and port of nodes, the counts of pending, started and completed executors per second, and the execution time and average start latency of executors on each node. You can navigate through the pages using the buttons placed at the bottom right of the table (**First, Previous, Next, Last**). Click on the column heading to ascend or descend the order of the listings.


### Members

Use this menu item to monitor each cluster member (node) and perform operations like running garbage collection (GC) and taking a thread dump. Once you select a member from the menu, a new tab for monitoring that member opens on the right, as shown below.

![](images/MembersHome.jpg)

The **CPU Utilization** chart shows the percentage of CPU usage on the selected member. The **Memory Utilization** chart shows the memory usage on the selected member with three different metrics (maximum, used and total memory). You can open both of these charts as separate windows using the ![](images/ChangeWindowIcon.jpg) button placed at top right of each chart; this gives you a clearer view of the chart.

The window titled **Partitions** shows which partitions are assigned to the selected member. **Runtime** is a dynamically updated window tab showing the processor number, the start and up times, and the maximum, total and free memory sizes of the selected member. Next to this, the **Properties** tab shows the system properties. The **Member Configuration** window shows the connected Hazelcast cluster's XML configuration.

Besides the aforementioned monitoring charts and windows, you can also perform operations on the selected member through this page. The operation buttons are located at the top right of the page, as explained below:

-	**Run GC**: When pressed, garbage collection is executed on the selected member. A notification stating that the GC execution was successful will be shown.
-	**Thread Dump**: When pressed, thread dump of the selected member is taken and shown as a separate dialog to the user.
-	**Shutdown Node**: It is used to shutdown the selected member.




### Scripting


You can use the scripting feature of this tool to execute codes on the cluster. To open this feature as a tab, select **Scripting** located at the toolbar on top. Once selected, the scripting feature opens as shown below.

![](images/scripting.jpg)

In this window, the **Scripting** part is the actual coding editor. You can select the members on which the code will execute from the **Members** list shown at the right side of the window. Below the members list, a combo box enables you to select a scripting language: currently, Javascript, Ruby, Groovy and Python languages are supported. After you write your script and press the **Execute** button, you can see the execution result in the **Result** part of the window.

There are **Save** and **Delete** buttons on the top right of the scripting editor. To save your scripts, press the **Save** button after you type a name for your script into the field next to this button. The scripts you saved are listed in the **Saved Scripts** part of the window, located at the bottom right of the page. Click on a saved script from this list to execute or edit it. If you want to remove a script that you wrote and saved before, select it from this list and press the **Delete** button.

In the scripting engine you have a `HazelcastInstance` bonded to a variable named `hazelcast`. You can invoke any method that `HazelcastInstance` has via the `hazelcast` variable. You can see example usage for JavaScript below.


```javascript
var name = hazelcast.getName();
var node = hazelcast.getCluster().getLocalMember();
var employees = hazelcast.getMap("employees");
employees.put("1","John Doe");
employees.get("1"); // will return "John Doe"
```


### Console

The Management Center has a console feature that enables you to execute commands on the cluster. For example, you can perform `put`s and `get`s on a map, after you set the namespace with the command `ns <name of your map>`. The same is valid for queues, topics, etc. To execute your command, type it into the field below the console and press **Enter**. Type `help` to see all the commands that you can use.

Open a console window by clicking on the **Console** button located on the toolbar. Below is a sample view with some executed commands.

![](images/console.jpg)



### Alerts

You can use the alerts feature of this tool to receive alerts by creating filters. In these filters, you can specify criteria for cluster, nodes or data structures. When the specified criteria are met for a filter, the related alert is shown as a pop-up message on the top right of the page.

Once you click the **Alerts** button located on the toolbar, the page shown below appears.

![](images/Alerts-Home.jpg)

**Creating Filters for Cluster**

Select the **Cluster Alerts** check box to create a cluster wise filter. Once selected, the next screen asks for the items for which alerts will be created, as shown below.

![](images/ClusterAlert1.jpg)

Select the desired items and click the **Next** button. On the next page (shown below), specify the frequency of checks in **hour** and **min** fields, give a name for the filter, select whether notification e-mails will be sent (to no one, only admin or to all users) and select whether the alert data will be written to the disk (if checked, you can see the alert log at the directory */users/<your user>/mancenter<version>*).

![](images/ClusterAlert2.jpg)

Click on the **Save** button; your filter will be saved and put into the **Filters** part of the page, as shown below.

![](images/ClusterAlert3.jpg)

To edit the filter, click on the ![](images/EditIcon.jpg) icon. To delete the filter, click on the ![](images/DeleteIcon.jpg) icon.

**Creating Filters for Cluster Members**

Select **Member Alerts** check box to create filters for some or all members in the cluster. Once selected, the next screen asks for which members the alert will be created. Select the desired members and click on the **Next** button. On the next page (shown below), specify the criteria.

![](images/MemberAlert1.jpg)

Alerts can be created when:

-	free memory on the selected nodes is less than the specified number.
-	used heap memory is larger than the specified number.
-	the number of active threads are less than the specified count.
-	the number of daemon threads are larger than the specified count.

When two or more criteria is specified they will be bound with the logical operator **AND**.

On the next page, give a name for the filter, select whether notification e-mails will be sent (to no one, only admin, or to all users) and select whether the alert data will be written to the disk (if checked, you can see the alert log at the directory */users/<your user>/mancenter<version>*).

Click on the **Save** button; your filter will be saved and put into the **Filters** part of the page. To edit the filter, click on the ![](images/EditIcon.jpg) icon. To delete it, click on the ![](images/DeleteIcon.jpg) icon.

**Creating Filters for Data Types**

Select the **Data Type Alerts** check box to create filters for data structures. The next screen asks for which data structure (maps, queues, multimaps, executors) the alert will be created. Once a structure is selected, the next screen immediately loads and you then select the data structure instances (i.e. if you selected *Maps*, it will list all the maps defined in the cluster, you can select one map or more). Select as desired, click on the **Next** button, and select the members on which the selected data structure instances will run.

The next screen, as shown below, is the one where you specify the criteria for the selected data structure.

![](images/DataAlert1.jpg)

As the screen shown above shows, you will select an item from the left combo box, select the operator in the middle one, specify a value in the input field, and click on the **Add** button. You can create more than one criteria in this page; those will be bound by the logical operator **AND**.

After you specify the criteria and click the **Next** button, give a name for the filter, select whether notification e-mails will be sent (to no one, only admin or to all users) and select whether the alert data will be written to the disk (if checked, you can see the alert log at the directory */users/<your user>/mancenter<version>*).

Click on the **Save** button; your filter will be saved and put into the **Filters** part of the page. To edit the filter, click on the ![](images/EditIcon.jpg) icon. To delete it, click on the ![](images/DeleteIcon.jpg) icon.



### Administration

![image](images/NoteSmall.jpg) ***NOTE:*** *This toolbar item is available only to admin users, i.e. the users who initially have ***admin*** as their both usernames and passwords.*

The **Admin** user can add, edit, and remove users and specify the permissions for the users of Management Center. To perform these operations, click on the **Administration** button located on the toolbar. The page below appears.

![](images/admin.jpg)

To add a user to the system, specify the username, e-mail and password in the **Add/Edit User** part of the page. If the user to be added will have administrator privileges, select **isAdmin** checkbox. **Permissions** checkboxes have two values:

-	**Read Only**: If this permission is given to the user, only *Home*, *Documentation* and *Time Travel* items will be visible at the toolbar at that user's session. Also, users with this permission cannot update a [map configuration](#map-config), run a garbage collection and take a thread dump on a node, or shutdown a node (please see [Members](#members) section).
-	**Read/Write**: If this permission is given to the user, *Home*, *Scripting*, *Console*, *Documentation* and *Time Travel* items will be visible. The users with this permission can update a map configuration and perform operations on the nodes.

After you enter/select all fields, click **Save** button to create the user. You will see the newly created user's username on the left side, in the **Users** part of the page.


To edit or delete a user, select a username listed in the **Users**. Selected user information appears on the right side of the page. To update the user information, change the fields as desired and click the **Save** button. To delete the user from the system, click the **Delete** button.



### Time Travel

Time Travel is used to check the status of the cluster at a time in the past. When this item is selected on the toolbar, a small window appears on top of the page, as shown below.

![](images/timetravel.jpg)

To see the cluster status in a past time, Time Travel should be enabled first. Click on the area where it says **OFF** (on the right of Time Travel window). It will turn to **ON** after it asks whether to enable the Time Travel with a dialog: click on **Enable** in the dialog to enable Time Travel.

Once it is **ON**, the status of your cluster will be stored on your disk as long as your web server is alive.

You can go back in time using the slider and/or calendar and check your cluster's situation at the selected time. All data structures and members can be monitored as if you are using the management center normally (charts and data tables for each data structure and members). Using the arrow buttons placed at both sides of the slider, you can go back or further with steps of 5 seconds. It will show status if Time Travel has been **ON** at the selected time in past; otherwise, all the charts and tables will be shown as empty.

The historical data collected with Time Travel feature are stored in a file database on the disk. These files can be found on the directory specified by `hazelcast.mancenter.home` (by default `mancenter3` directory in the user home folder).

### Documentation

To see the documentation, click on the **Documentation** button located at the toolbar. Management Center manual will appear as a tab.

### Suggested Heap Size

**For 2 Nodes**

| Mancenter Heap Size | # of Maps | # of Queues | # of Topics |
| -------- | --------- | ---------- | ------------ |
| 256m | 3k | 1k | 1k |
| 1024m | 10k | 1k | 1k |

**For 10 Nodes**

| Mancenter Heap Size | # of Maps | # of Queues | # of Topics |
| -------- | --------- | ---------- | ------------ |
| 256m | 50 | 30 | 30 |
| 1024m | 2k | 1k | 1k | 

**For 20 Nodes**

| Mancenter Heap Size | # of Maps | # of Queues | # of Topics |
| -------- | --------- | ---------- | ------------ |
| 256m* | N/A | N/A | N/A |
| 1024m | 1k | 1k | 1k |

\* With 256m heap, management center is unable to collect statistics.

<br> </br>
