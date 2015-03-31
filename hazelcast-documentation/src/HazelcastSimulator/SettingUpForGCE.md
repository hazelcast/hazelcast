

## Setting Up For Google Compute Engine

To prepare the Simulator for testing a Hazelcast cluster deployed at Google Compute Engine (GCE), first you need an e-mail address to be used as a GCE service account. You can obtain this e-mail address in the Admin GUI console of GCE. In this console, select **Credentials** in the menu **API &  Auth**. Then, click the **Create New Client ID** button and select **Service Account**. Usually, this e-mail address is in this form: `<your account ID>@developer.gserviceaccount.com`.

Save the **p12** keystore file that you obtained while creating your Service Account (you will refer to that path). In the `bin` folder of the Hazelcast Simulator package that you downloaded, edit the `setupGce.sh` script to specify the following parameters:

- GCE_id: Your developer e-mail address that you obtained in the Admin GUI console of GCE.
- p12File: The path to your p12 file you saved while you were obtaining your developer e-mail address.

After you run the edited `setupGce.sh` script, the `simulator.properties` file that you need for a proper testing of your instances on GCE is created in the `conf` folder of Hazelcast Simulator.
