from template.table import Table, Record
from template.index import Index
# from template.quecc import QueCC

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))
        # print("query: ", query, " args: ", args)

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            if result == False:
                return False
            # print("result: ", result)
        return True

    # def abort(self):
    #     #TODO: do roll-back and any other necessary operations
    #     return False
    #
    # def commit(self):
    #     # TODO: commit to database
    #     return True
