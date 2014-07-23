## Queues

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
