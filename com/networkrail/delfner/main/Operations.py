from pyspark.sql.utils import AnalysisException
from com.networkrail.delfner.main.sparksession import spark
from pyspark.sql.functions import col


def tableExists(dbName, tblName):
    """
    desc:
        A static function for checking whether a given table exists.
        spark.catalog.tableExists method does not exist in python
        Although the scala method can be accessed using _jsparkSession, it is better to avoid using it

    args:
        dbName: String
        tblName: String

    return:
        Boolean - returns True or if it doesn't exists raises an exception

    example:
        tableExists("bronze_mp1", "vbak")

    tip:
        N/A
    """
    try:
        spark.read.table(dbName + "." + tblName)
        return True
    except AnalysisException:
        return False


def createDeltaTable(df, path, dbName, tblName):
    """
    A Function for creating the target table using the given dataframe schema, database and table name
    """
    tblDDL = df._jdf.schema().toDDL()

    tblProps = "delta.autoOptimize.autoCompact = false, \n\
             delta.autoOptimize.optimizeWrite = true, \n\
             delta.tuneFileSizesForRewrites = true, \n\
             delta.enableChangeDataCapture = true"

    createTable = "CREATE TABLE IF NOT EXISTS {dbName}.{tblName} \n ({tblDDL}) \n USING DELTA \n LOCATION \"{path}\" \n TBLPROPERTIES ({tblProps})".format(
        dbName=dbName,
        tblName=tblName,
        tblDDL=tblDDL,
        path=path,
        tblProps=tblProps
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbName}")

    spark.sql(createTable)
