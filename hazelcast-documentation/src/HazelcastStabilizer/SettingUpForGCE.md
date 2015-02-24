

## Setting Up For Google Compute Engine

For setup on GCE a developer email can be obtained from the GCE admin GUI console. Its usually something in the form: @developer.gserviceaccount.com go to API & Auth > Credentials, click Create New Client ID, select Service Account. save your p12 keystore file that is obtained when creating a "Service Account"

run ./setupGce.sh script provide you @developer.gserviceaccount.com as 1st argument provide and the path to your p12 file as 2nd argument the setupGce.sh script will create your stabilizer.properties, in the conf directory