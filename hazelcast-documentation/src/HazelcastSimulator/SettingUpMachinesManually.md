

## Setting Up Machines Manually

You may want to set up Hazelcast Simulator on the environments different than your clusters placed on a cloud such as your local machines, test laboratory, etc. In this case, perform the following steps.

1. Copy the `STABILIZER_HOME/conf/simulator.properties` to your working directory.

2. Edit the `USER` in the `simulator.properties` file if you want to use a different user name than `simulator`.

3. Create an RSA key pair or use an existing one. The usage of the key should not require to enter the pass-phrase manually. A key with pass-phrase and ssh-agent-forwarding is strongly recommended, but a key without a pass-phrase will also work.

 You can check whether a key pair exists with this command:

 ```
 ls -al ~/.ssh
 ```
 If it does not exist, you can create a key pair on the client machine with this command:

 ```
 ssh-keygen -t rsa
 ```
 
 You will get a few more questions:

 	* Enter file in which to save the key (/home/demo/.ssh/id_rsa):
 	* Enter passphrase (empty for no passphrase): (It is optional)

4. Copy the public key into the `~/.ssh/authorized_keys` file on the remote machines with this command:

 ```
 ssh-copy-id user@123.45.56.78
 ```

5. Create the `agents.txt` file and add the IP addresses of the machines. The content of the `agents.txt` file with the IP addresses added looks like the following:

 ```
 98.76.65.54
 10.28.37.46
 ```

6. Run the command `provisioner --restart` to verify.


![image](images/NoteSmall.jpg) ***NOTE***: *For the full description of `simulator.properties` file, please refer to the [Simulator.Properties File Description section](#simulator-properties-file-description).*


