## Introduction

Hazelcast Hosted Management Center enables you to monitor and manage your servers running Hazelcast. In addition to monitoring overall state of your clusters, you can also analyze and browse your data structures in detail. You can also update map configurations and take thread dump from nodes. With its scripting module, you can run scritps (JavaScript, Groovy etc.) on your servers. Version 2.0 is a web based tool so you can deploy it into your internal server and serve your users.

## Installation

It is important to understand how it actually works. Basically you will deploy `mancenter`-*version*`.war` application into your Java web server and then tell Hazelcast nodes to talk to that web application. That means, your Hazelcast nodes should know the URL of `mancenter` application before they start.

Here are the steps:

-   Download the latest Hazelcast zip from [hazelcast.org](http://www.hazelcast.org/download/).

-   Zip contains `mancenter`-*version*`.war` file. Deploy it to your web server (Tomcat, Jetty etc.) Let's say it is running at`http://localhost:8080/mancenter-`*version*

-   Start your web server and make sure `http://localhost:8080/mancenter`-*version*` is up.

-   Configure your Hazelcast nodes by adding the URL of your web app to your `hazelcast.xml`. Hazelcast nodes will send their states to this URL.

```xml
<management-center enabled="true">http://localhost:8080/mancenter-version</management-center>
```
-   Start your Hazelcast cluster.

*Management Center creates a directory with name "mancenter" under your "user/home" directory to save data files. You can change the data directory setting "hazelcast.mancenter.home" system property.*


## Login & Sign Up
After the cluster has started, go to `http://localhost:8080/mancenter`-*version* using any web browser. Below page will load.

![](images/1Login.jpg)

**Initial login username/passwords is `admin/admin`**
???
???

Once the credentials are entered and **Login** key is pressed, the tool will ask from the user to choose a cluster, as shown below.

![](images/2SelectCluster.jpg)

Select the cluster and hit **Connect** button.


## User Administration

Default credentials are for the admin user. In the `Administration` tab, Admin can add/remove/update users and control user read/write permissions.

![](images/admin.jpg)

## Tool Overview

Once the page is loaded after selecting a cluster, tool's home page appears as shown below.

![](images/3HomePage.jpg)

This page provides the fundamental properties of the selected cluster which are explained in the below subsections.

It also has a toolbar on the top and a menu on the left.

####Toolbar
Toolbar has the following buttons:

-	**Home**: When pressed, loads the home page shown above.
-	**Scripting**: When pressed, loads the page used to write and execute user`s own scripts on the cluster. Please see [Scripting](#scripting).
-	**Console**: When pressed, loads the page used to execute commands on the cluster. Please see [Console](#console).
-	**Documentation**: It is used to open the documentation of Hosted Management Center in a window inside the tool. Please see [Documentation](#documentation).
-	**Cluster Selector**: It is used to switch between clusters. When the mouse is moved onto this item, a dropdown list of clusters appears.

     ![](images/4ChangeCluster.jpg)
     
     The user can select any cluster and once selected, the page immediately loads with the selected cluster's information.
-	**Logout**: 


The starter page of the tool is`Cluster Home`. Here you can see cluster's main properties such as uptime, memory. Also with pie chart, you can see the distribution of partitions over cluster members. You can come back to this page, by clicking the `Home` icon on the top-right toolbar. On the left panel you see the Map/Queue/Topic instances in the cluster. At the bottom-left corner, members of the cluster are listed. On top menu bar, you can change the current tab to`Scripting, Docs`, user`Administration`. Note that Administration tab is viewable only for admin users. Also `Scripting` page is disabled for users with read-only credential.





## Maps

Map instances are listed on the left panel. When you click on a map, a new tab for monitoring this map instance is opened on the right. In this tab, you can monitor metrics also re-configure the map.

![](images/MapsHome.jpg)

### Monitoring Maps

In map page you can monitor instance metrics by 2 charts and 2 datatables. First data table "Memory Data Table" gives the memory metrics distributed over members. "Throughput Data Table" gives information about the operations performed on instance (get, put, remove) Each chart monitors a type data of the instance on cluster. You can change the type by clicking on chart. The possible ones are: Size, Throughput, Memory, Backup Size, Backup Memory, Hits, Locked Entries, Puts, Gets, Removes...

![](images/mapchart.jpg)

#### Size
???
#### Throughput
???
#### Memory
???
#### Backup Size
???
#### Backup Memory
???
#### Hits
???
#### Locked Entries
???
#### Puts/s
???
#### Gets/s
???
#### Removes/s
???



### Map Browser

You can open "Map Browser" tool by clicking "Browse" button on map tab page. Using map browser, you can reach map's entries by keys. Besides its value, extra informations such as entry's cost, expiration time is provided.

[![](images/mapbrowse.jpg)](images/mapbrowse.jpg)

### Map Config

You can open "Map Configuration" tool by clicking "Config" button on map tab page. This button is disabled for users with Read-Only permission. Using map config tool you can adjust map's setting. You can change backup count, max size, max idle(seconds), eviction policy, cache value, read backup data, backup count of the map.

![](images/mapconfig.jpg)


## Queues

Queues is the second data structure that you can monitor in management center. You can activate the Queue Tab by clicking the instance name listed on the left panel under queues part. The queue page consists of the charts monitoring data about the queue. You can change the data to be monitored by clicking on the chart. Available options are Size, Polls, Offers.

![](images/queue.jpg)


## Topics

You can monitor your topics' metrics by clicking the topic name listed on the left panel under topics part. There are two charts which reflects live data, and a datatable lists the live data distributed among members.

![](images/topic.jpg)


## Members

The current members in the cluster are listed on the bottom side of the left panel. You can monitor each member on tab page displayed by clicking on member items.

![](images/member.jpg)

### Monitoring

In members page there are 4 inner tab pages to monitor meber's state and properties. Runtime: Runtime properties about memory, threads are given. This data updates dynamically. Properties: System properties are displayed. Configuration: Configuration xml initially set can be viewed here. Partitions: The partitions belongs to this member are listed.

![](images/memberconf.jpg)

### Operations

Besides monitoring you can perform certain actions on members. You can take thread dump of the member and you can perform garbage collection on the selected member.

![](images/mapoperations.jpg)


## System Logs

System logs part helps you track internal operations and detect problems. To see system logs first you should set a Log level other than "None". In left menu there are inputs by which you can filter the displayed logs dynamically. Also you can Export your logs and send the file to Hazelcast support team, so they can analyze and help you solving your problem.

![](images/systemlogs.jpg)


## Scripting

In scripting part, you can execute your own code on your cluster. In the left part you can select members, on which the code will be executed. Also you can select over scripting languages: Javascript, Groovy, JRuby, BeanShell. This part is only enabled for users with read/write permissions for current cluster.

![](images/scripting.jpg)


## Time Travel

Time Travel mode is activated by clicking clock icon on top right toolbar. In time travel mode, the time is paused and the full state of the cluster is displayed according the time selected on time slider. You can change time either by Prev/Next buttons or sliding the slider. Also you can change the day by clicking calendar icon. Management center stores the states in you local disk, while your web server is alive. So if you slide to a time when you do not have data, the reports will be seen as empty.

![](images/timetravel.jpg)


## Console

The console tool enables you execute commands on the cluster. You can read or write on instances but first you should set namespace. For example if you have a map with name "mapCustomers". To get a customer with key "Jack" you should first set the namespace with command "ns mapCustomers". Then you can take the object by "m.get Jack" Here is the command list:

```
-- General commands
echo true|false                      //turns on/off echo of commands (default false)
silent true|false                    //turns on/off silent of command output (default false)
<number> <command>                  //repeats <number> time <command>, replace $i in <command> with current iteration (0..<number-1>)
&<number> <command>                  //forks <number> threads to execute <command>, replace $t in <command> with current thread number (0..<number-1>
     When using #x or &x, is is advised to use silent true as well.
     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a start key index is specified
jvm                                  //displays info about the runtime
who                                  //displays info about the cluster
whoami                               //displays info about this cluster member
ns <string>                          //switch the namespace for using the distributed queue/map/set/list <string> (defaults to "default"
@<file>                              //executes the given <file> script. Use '//' for comments in the script

-- Queue commands
q.offer <string>                     //adds a string object to the queue
q.poll                               //takes an object from the queue
q.offermany <number> [<size>]        //adds indicated number of string objects to the queue ('obj<i>' or byte[<size>])
q.pollmany <number>                  //takes indicated number of objects from the queue
q.iterator [remove]                  //iterates the queue, remove if specified
q.size                               //size of the queue
q.clear                              //clears the queue

-- Set commands
s.add <string>                       //adds a string object to the set
s.remove <string>                    //removes the string object from the set
s.addmany <number>                   //adds indicated number of string objects to the set ('obj<i>')
s.removemany <number>                //takes indicated number of objects from the set
s.iterator [remove]                  //iterates the set, removes if specified
s.size                               //size of the set
s.clear                              //clears the set

-- Lock commands
lock <key>                           //same as Hazelcast.getLock(key).lock()
tryLock <key>                        //same as Hazelcast.getLock(key).tryLock()
tryLock <key> <time>                 //same as tryLock <key> with timeout in seconds
unlock <key>                         //same as Hazelcast.getLock(key).unlock()

-- Map commands
m.put <key> <value>                  //puts an entry to the map
m.remove <key>                       //removes the entry of given key from the map
m.get <key>                          //returns the value of given key from the map
m.putmany <number> [<size>] [<index>]//puts indicated number of entries to the map ('key<i>':byte[<size>], <index>+(0..<number>)
m.removemany <number> [<index>]      //removes indicated number of entries from the map ('key<i>', <index>+(0..<number>)
     When using &x with m.putmany and m.removemany, each thread will get a different share of keys unless a start key <index> is specified
m.keys                               //iterates the keys of the map
m.values                             //iterates the values of the map
m.entries                            //iterates the entries of the map
m.iterator [remove]                  //iterates the keys of the map, remove if specified
m.size                               //size of the map
m.clear                              //clears the map
m.destroy                            //destroys the map
m.lock <key>                         //locks the key
m.tryLock <key>                      //tries to lock the key and returns immediately
m.tryLock <key> <time>               //tries to lock the key within given seconds
m.unlock <key>                       //unlocks the key

-- List commands:
l.add <string>
l.add <index> <string>
l.contains <string>
l.remove <string>
l.remove <index>
l.set <index> <string>
l.iterator [remove]
l.size
l.clear
-- IAtomicLong commands:
a.get
a.set <long>
a.inc
a.dec
-- Executor Service commands:
execute <echo-input>                //executes an echo task on random member
execute0nKey    <echo-input> <key>      //executes an echo task on the member that owns the given key
execute0nMember <echo-input> <key>  //executes an echo task on the member with given index
execute0nMembers <echo-input>       //executes an echo task on all of the members
```
![](images/console.jpg)
