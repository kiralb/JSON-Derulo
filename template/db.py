from template.table import Table
import os
import pickle

class Database():

    def __init__(self):
        self.tables = []
        pass



    def open(self, path):
        path = "ECS165"
        try:
            os.mkdir(path)
        except OSError:
            print("creation of dir", path, "already exists")
        else:
            print("creation of dir ", path, "passed")
        pass



    """

    Flush changes back to disk after close is called

    """
    def close(self):
        globalPath = "/Users/BrianNguyen/Documents/GitHub/JSON-Derulo/ECS165/"
        for i in range(len(self.tables)):
            path = self.tables[i].name + ".pkl"
            f = open(globalPath + path, "wb")
            pickle.dump(self.tables[i] , f)
            f.close()


    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        self.tables.append(table)
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
        globalPath = "/Users/as/Documents/ecs165a/milestone1/JSON-Derulo/ECS165/"
        path = name + ".pkl"
        print(path)
        f = open(globalPath + path, "rb")
        dict_x = pickle.load(f)
        print(dict_x)
        print(dict_x.num_columns)
        # print ("key {}, value {} ". format(0,  dict_x[0]))
        f.close()
