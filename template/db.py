from template.table import Table
import os

class Database():

    def __init__(self):
        self.tables = []
        pass


    """

    Initialize bufferpool here
    Refer to top of page 2 of Milestone2.pdf

    """
    def open(self, path):
        path = "ECS165"
        try:
            os.mkdir(path)
        except OSError:
            print("creation of dir", path, "failed")
        else:
            print("creation of dir ",path," passed")
        pass



    """

    Flush changes back to disk after close is called

    """
    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
