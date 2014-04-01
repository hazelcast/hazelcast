
## Alerts

Alerts feature of this tool is used to receive alerts by creating filters. In these filters, criteria can be specified for cluster, nodes or data structures. When the specified criteria are met for a filter, related alert is shown as a pop-up message on top right of the page. 

Once the **Alerts** button located at the toolbar is clicked, the page shown below appears. 

![](images/Alerts-Home.jpg)

**Creating Filters for Cluster**

Select **Cluster Alerts** check box to create a cluster wise filter. Once selected, next screen asks the items for which alerts will be created, as shown below.

![](images/ClusterAlert1.jpg)

Select the desired items and click the **Next** button. On the next page shown below, specify the frequency of checks in **hour** and **min** fields, give a name for the filter, select whether notification e-mails will be sent (to no one, only admin or to all users) and select whether the alert data will be written to the disk (if checked, you can see the alert log at the directory */users/<your user>/mancenter<version>*).

![](images/ClusterAlert2.jpg)

Click on the **Save** button; your filter will be saved and put into the **Filters** part of the page, as shown below.

![](images/ClusterAlert3.jpg)

You can edit the filter by clicking on the ![](images/EditIcon.jpg) icon and delete it by clicking on the ![](images/DeleteIcon.jpg) icon.

**Creating Filters for Cluster Members**

Select **Member Alerts** check box to create filters for some or all members in the cluster. Once selected, next screen asks for which members the alert will be created. Select as desired and click on the **Next** button. On the next page shown below, specify the criteria. 

![](images/MemberAlert1.jpg)

Alerts can be created when:

-	free memory on the selected nodes is less than the specified number
-	used heap memory is larger than the specified number
-	number of active threads are less than the specified count
-	number of daemon threads are larger than the specified count

When two or more criteria is specified they will be bound with the logical operator **AND**.

On the next page, give a name for the filter, select whether notification e-mails will be sent (to no one, only admin or to all users) and select whether the alert data will be written to the disk (if checked, you can see the alert log at the directory */users/<your user>/mancenter<version>*).

Click on the **Save** button; your filter will be saved and put into the **Filters** part of the page. You can edit the filter by clicking on the ![](images/EditIcon.jpg) icon and delete it by clicking on the ![](images/DeleteIcon.jpg) icon.

**Creating Filters for Data Types**

Select **Data Type Alerts** check box to create filters for data structures. Next screen asks for which data structure (maps, queues, multimaps, executors) the alert will be created. Once a structure is selected, next screen immediately loads and wants you to select the data structure instances (i.e. if you selected *Maps*, it will list all the maps defined in the cluster, you can select only one map or more). Select as desired, click on the **Next** button and select the members on which the selected data structure instances run. 

Next screen, as shown below, is the one where the criteria for the selected data structure are specified.

![](images/DataAlert1.jpg)

As it can be seen, you will select an item from the left combo box, select the operator in the middle one, specify a value in the input field and click on the **Add** button. You can create more than one criteria in this page, and those will be bound by the logical operator **AND**.

After the criteria are specified and **Next** button clicked, give a name for the filter, select whether notification e-mails will be sent (to no one, only admin or to all users) and select whether the alert data will be written to the disk (if checked, you can see the alert log at the directory */users/<your user>/mancenter<version>*).

Click on the **Save** button; your filter will be saved and put into the **Filters** part of the page. You can edit the filter by clicking on the ![](images/EditIcon.jpg) icon and delete it by clicking on the ![](images/DeleteIcon.jpg) icon.

