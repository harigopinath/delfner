import logging
from com.networkrail.delfner.main.sparksession import spark
from com.networkrail.delfner.main.Utils import pathExists


class ArchiveFiles():
    """
    A pipeline for Archiving the files from given path to the archive path

    args:
        dbName: String - Database Name of Delta table
        tblName: String - Table Name of Delta table
        columnName: String - A column on the table of date or timestamp type
        thresholdDays: String - number of days beyond which data should be archived
        archivePath: String - path where the old data should be archived

    methods:
        archiveDelta

        dropOldData

    How to call:
        archiveObj = ArchiveFiles("default", "some_Table_name","column_name", "800", "abfss://somePath" )

        archiveObj.archiveDelta()
    """

    def __init__(self, dbName, tblName, columnName, thresholdDays, archivePath):
        """
        desc:
            Initialize the required class variables

        args:
            dbName: String - Database Name of Delta table
            tblName: String - Table Name of Delta table
            columnName: String - A column on the table of date or timestamp type
            thresholdDays: String - number of days beyond which data should be archived
            archivePath: String - path where the old data should be archived

        return:
            Does not return any value
        """

        self.dbName = dbName
        self.tblName = tblName
        self.columnName = columnName
        self.thresholdDays = thresholdDays
        self.archivePath = archivePath
        self.df = spark.table(self.dbName + "." + self.tblName) \
            .filter(f"{self.columnName} <= date_trunc('day', current_timestamp()) - INTERVAL {self.thresholdDays} DAYS")
        self.recCount = self.df.count()

        print("INFO: Initialised the class variables")

    def archiveDelta(self):
        """
        desc:
            Writes the old/archived data of the delta table to a separate archive location

        args:
            No arguments - just makes use of class variables

        return:
            Returns nothing - Writes the data to a delta path using append mode

        """
        print("INFO: Writing the old data from delta table to archive location")

        try:
            self.df.write.format("delta").mode("append").save(self.archivePath)
        except Exception as e:
            print("ERROR: Unable to write to Archive path - check the location")
            raise Exception(e)

        print(f"INFO: Total no. of records Archived: {self.recCount}")

    def dropOldData(self):
        """
        desc:
            Deletes the old/archived data from the original delta table

        args:
            No arguments - just uses the class variables

        return:
            Returns nothing - Just deletes the data beyond a certain period on the delta table
        
        """
        srcCount = self.recCount
        tgtCount = self.archiveRecCount()

        if srcCount != tgtCount:
            print(f"WARN: Total number of records archived: {srcCount} \n \
                   WARN: Total number of records deleted: {tgtCount} \n \
                   ERROR: The Archived records is not equal to the deleted records - check the condition and run again")

        if pathExists(self.archivePath) & (srcCount == tgtCount):
            spark.sql(f"DELETE FROM {self.dbName}.{self.tblName} WHERE {self.columnName} <= date_trunc('day', current_timestamp()) - INTERVAL {self.thresholdDays} DAYS")

            spark.sql(f"ALTER TABLE {self.dbName}.{self.tblName} SET TBLPROPERTIES(delta_archive_path = '{self.archivePath}')")

            print(f"INFO: Total number of records deleted: {tgtCount}")

    def archiveRecCount(self):
        #Count the number of output rows from the recent version of the archived delta table
        cnt = spark.sql(f"DESCRIBE HISTORY delta.`{self.archivePath}`").select("operationMetrics.numOutputRows").limit(1).collect()[0][0]
  
        return int(cnt)
