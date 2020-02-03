from template.table import Table, Record
from template.index import Index
from template.page import *
import sys


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    #rowCounter = 0
    def __init__(self, table):
        self.table = table
        self.basePageRows = 0
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass
    
    def addToByteArray(self, physicalPage, attribute, column):
        pass
        #Need to confir this adds to the bite array
        byteArray = physicalPage.data 
        attribute = str(attribute)



        currentIndex = physicalPage.num_records *physicalPage.offset
        print("len(attribute) + currentIndex: ", len(attribute) + currentIndex, "\n")

        if( len(attribute) + currentIndex > 4096 ):
            print("Overflow, Exiting Function")
            self.basePageRows = self.basePageRows + 1
            self.table.addNewSetOfPhysicalBasePages()
            # start adding to this new set of physical base pages

            physicalPage = self.table.basePages[self.basePageRows][column]
            if(len(str(attribute)) < 3):
                physicalPage.offset = 3
            else:
                physicalPage.offset = len(str(attribute))
            byteArray = physicalPage.data 


        i = physicalPage.offset * physicalPage.num_records
        j = 0
        while(j < len(attribute)):
            byteArray[i] = int(attribute[j])
            #byteArray[i] = int(attribute[j])
            
            i = i + 1
            j = j + 1
        #print(byteArray)
        physicalPage.num_records += 1
    
    
    def addToBasePages(self, attribute, column):
        pass
        physicalPage = self.table.basePages[self.basePageRows][column]
        if(len(str(attribute)) < 3):
            physicalPage.offset = 3
        else:
            physicalPage.offset = len(str(attribute))
        self.addToByteArray(physicalPage, attribute, column)


        
    
    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        pass
        schema_encoding = '0' * self.table.num_columns
        for i in range(len(columns)):
           # add first parameter to bp5
           # add second parameter to bp 6
           # add third parameter to bp 7 ...
           attribute = columns[i]
           self.addToBasePages(attribute, i)

           #print("Printing the bite array: ", physicalPage.data, "\n")

        
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