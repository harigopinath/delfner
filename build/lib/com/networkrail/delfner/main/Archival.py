from com.networkrail.delfner.main.sparksession import spark
from delta.tables import DeltaTable
from com.networkrail.delfner.main.Utils import pathExists


class ArchiveFiles:
    """
    A pipeline for Archiving the files from given path to the archive path

    args:
        dbName: String - Database Name of Delta table
        tblName: String - Table Name of Delta table
        thresholdDays: String - number of days beyond which data should be archived
        archivePath: String - path where the old data should be archived
        //
        srcPath: String - source path where files arrive periodically
        archivePath: String - path where all the source files are archived
        fileType: String - file type or extension, ex: csv, json, avro,...
        filePattern: String - regex for identifying source files to be archived (optional)

    methods:
        archiveDelta

        dropOldData

    How to call:
        archiveObj = ArchiveFiles("default", "some_Table_name", "800", "abfss://somePath" )

        archiveObj.archiveDelta


    """

    def __init__(self, dbName, tblName, thresholdDays, archivePath):
        """
        desc:
            Initialize the required class variables

        args:
            dbName: String - Database Name of Delta table
            tblName: String - Table Name of Delta table
            thresholdDays: String - number of days beyond which data should be archived
            archivePath: String - path where the old data should be archived

        return:
            Does not return
        """

        self.dbName = dbName
        self.tblName = tblName
        self.thresholdDays = thresholdDays
        self.archivePath = archivePath
        self.df = spark.table(self.dbName, self.tblName) \
            .filter(f"createDate >= current_timestamp() - INTERVAL {self.thresholdDays} DAYS")

    def archiveDelta(self, df):
        """
        desc:
            Writes the old/archived data of the delta table to a separate archive location

        """
        df.write.format("delta").mode("append").save(self.archivePath)

    def dropOldData(self):
        """
        desc:
            Deletes the old/archived data from the original delta table

            something else I write
        """

        deltaTable = DeltaTable.forName(spark, self.dbName + "." + self.tblName)

        if pathExists(self.archivePath):
            deltaTable.delete("createDate >= current_timestamp() - INTERVAL {self.thresholdDays} DAYS")

            spark.sql(f"ALTER TABLE SET TBLPROPERTIES(delta_archive_path = {self.archivePath})")

    if __name__ == '__main__':
        archiveDelta()

        dropOldData()
