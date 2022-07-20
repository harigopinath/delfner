import dbutils
import glob


def pathExists(tablePath):
    """
    A Function for determining whether a table path already exists. The assumption is, if the table path doesn't exists, then this is the first run else it is not.
    """
    try:
        dbutils.fs.ls(tablePath)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise90


def srcPathExists(srcPath):
    """
    A Function for determining whether a srcPath already exists.
    """
    # This glob
    fileList = glob.glob(srcPath)
    if not fileList:
        return False
    else:
        return True
