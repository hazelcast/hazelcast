
## Executors
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
