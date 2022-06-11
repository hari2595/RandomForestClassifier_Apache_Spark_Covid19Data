# RandomForestClassifier_Apache_Spark_Covid19Data
<br>
A prediction model built on Toronto Public health. We load the Data into Google Cloud storage and then export it to Goole BigQuery and the compute engine with HDFS and Spark capabilities. Later we connect BigQuery Table to Tableau for some data exploration. Next we use the Scala script attached to predict the number of patients who are going to be admitted in the ICU while affected with covid.
<br>
<br>
<p>
    <img src="https://raw.githubusercontent.com/hari2595/RandomForestClassifier_Apache_Spark_Covid19Data/main/GCP%20horizontal%20framework.jpeg" height="440" />
</p>
<h3> Pre Requisites: </h3><br>
GCP Account <br>
Tableau <br>
<br>
<h3> Execution </h3>
<br>
<br>
Step 1: Login Into GCP. <br><br>
Step 2: Load the data into a landing bucket in Cloud Storage. <br><br>
Step 3: Create a cluster in data proc and launch it with Comput engine SSH connection. <br><br>
Step 4: Load the data from your landing bucket into your HDFS cluster<br><br>
Step 5: Open spark interface, enter into the paste mode and enter the scala script, mention the file path and exit the paste mode.<br><br>
Step 6: Export the result to abother final bucket.<br><br>
Step 7: Connect the BigQuery Table into your Tbaleau.<br><br>
Step 8: Now we can do some exploratory research on tableau, an example is given in the report above.<br><br>
