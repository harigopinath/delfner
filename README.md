# DELfNeR <br>

### A Data Engineering Library for Network Rail

**Description:** <br>
  <tab> A common python library for building scalable, reusable classes/methods for driving data engineering workloads at Network Rail

**General Conventions**
  Modules:
  Operations: Static functions typically involving applying transformation to a given dataframe
  Utils: static functions that does not involve spark
  Archival: A class for a specific functionality - archiving old data from delta tables
  Operations_test: Unit tests for functions in Operations
  Utils_test: Unit tests for functions in Utils
  
  *As a best practice, always write relevant unit tests for any new function added to Operations or Utils module
  ** Integration tests are required for classes, for example - Archival
  
**Steps to setup the code in your local system:**

1. Download and install VSCode (If required, raise a support ticket to get it installed)
2. Open VSCode, clock on Source Control tab on the left and clone this repository <br> **Note:** You should have configured Azure repos in your VSCode earlier.
3. While cloning the repository, it would have prompted you to choose a folder locally which will be your main project folder
4. Click on the View menu -> Terminal
5. Setup a python virtual environment and install the required libraries: <br>
    Make sure that the versions that you have used above, match with the versions of your Databricks cluster. Check the below [link](https://docs.microsoft.com/en-us/azure/databricks/release-notes/runtime/releases) for more information on DBR release notes
    python3.8 -m venv venv <br>
    source venv/bin/activate <br>
    
    pip install pyspark==3.2.1 <br>
    pip install delta-spark==1.1.1 <br>
    pip install delta==1.1.1 <br>
    pip install databricks-cli <br>
    pip install dbx

6. Create a new branch and work on any changes required for your specific use case or create new methods/classes that can be reused by other teams
  
7. Only team leads will be allowed to review the changes in a branch and merge it to the master branch

8. To build the whl file, run the below command in the terminal: <br>
    python setup.py bdist_wheel

  
  Steps to install the latest library in your databricks cluster?
  
  > 1. Navigate to the master branch <br>
  > 2. Navigate to the folder 'dist'
  > 3. Download the latest whl file
  > 4. Navigate to your databricks cluster, click on the libraries tab
  > 5. Click on Install New and upload the whl file
  > 6. Restart the cluster for uninstalling the old whl file and reinstalling it
  
 ## Archival of Delta Table - High Level Design
 
 ![Archive](/images/Archival_Design.png)
