
## Administration

***Note:*** *This toolbar item is available only to admin users, i.e. the users who initially have ***admin*** as their both usernames and passwords.*

**Admin** user can add, edit, remove users and specify the permissions for the users of Management Center. To perform these operations, click on **Administration** button located at the toolbar. The page shown below appears.

![](images/admin.jpg)

To add a user to the system, specify the username, e-mail and password in the **Add/Edit User** part of the page. If the user to be added will have administrator privileges, select **isAdmin** checkbox. **Permissions** checkboxes have two values:

-	**Read Only**: If this permission is given to the user, only *Home*, *Documentation* and *Time Travel* items will be visible at the toolbar at that user's session. Also, the users with this permission cannot update a [map configuration](#map-config), run a garbage collection and take a thread dump on a node, and shutdown a node (please see [Members](#members) section).
-	**Read/Write**: If this permission is given to the user, *Home*, *Scripting*, *Console*, *Documentation* and *Time Travel* items will be visible. The users with this permission can update a map configuration and perform operations on the nodes.

After all fields are entered/selected, click **Save** button to create the user. You will see the newly created user's username on the left side, in the **Users** part of the page.


To edit or delete a user, select a username listed in the **Users**. Selected user's information will appear on the right side of the page. To update the user information, change the fields as desired and click **Save** button. To delete the user from the system, click **Delete** button.
