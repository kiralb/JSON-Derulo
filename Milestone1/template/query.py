from template.table import Table, Record
from template.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    #rowCounter = 0
    def __init__(self, table):
        self.table = table
        self.rowCounter = 0
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass
	
	# attribute = 9066596710
	# i = 0
    def addToBasePages(self, attribute, i):
		# add to basePage[5 + i]
        print("accessing physical page: ", 4 + i)
        physicalPage = self.table.basePages[4 + i]
        if (len(str(attribute)) < 3):
			physicalPage.offset = 3
		else:
			physicalPage.offset = len(str(attribute))
        
		
	
    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        for i in range(len(columns)):
           # add first parameter to bp5
           # add second parameter to bp 6
           # add third parameter to bp 7 ...
           attribute = columns[i]
           self.addToBasePages(attribute, i)
        
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
