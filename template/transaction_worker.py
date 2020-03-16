from template.table import Table, Record
from template.index import Index
from template.query import Query
import threading
# from template.queCC import QueCC

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, table, twIndex, quecc, transactions = []):
        self.transactionWorkerNum = twIndex
        self.stats = []
        self.transactions = transactions
        self.table = table
        self.result = 0
        self.quecc = quecc
        self.queuesExecuted = 0
        self.lock = threading.Lock()
        pass

    def add_transaction(self, t):
        self.transactions.append(t)
        self.result = self.result + 1
        #print("we have added one to result: ", self.result)


    def sortIntoQueue(self, query):
        key = query[1][0]
        # print("key: ", key)
        RID = self.table.keyToRID[key]
        # print("RID: ", RID)
        column = RID // 2501
        row = self.transactionWorkerNum
        # print("query: ", query[0] ," RID: ", RID, "row: ", self.transactionWorkerNum, " column: ", column )
        self.quecc.setsOfQueues[row][column].put(query)
        # print("inserting: ", self.quecc.setsOfQueues[row][column])



    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # transaction_worker = TransactionWorker([t])
    """
    def run(self):
        # print("gets here")
        for transaction in self.transactions:
            # print("transaction: ", transaction.queries)
            # uncomment this and uncomment self.stats.append(True) if everything goes to sh*t
            self.stats.append(True)
            # sort queries in each transaction into respective queues
            for query in transaction.queries:
                # sort queries into the 16 queues
                # print("query: ", query)
                self.lock.acquire()
                self.sortIntoQueue(query)
                self.lock.release()
        self.quecc.plannerThreadsRun += 1
        # execute priority queues
        while (self.quecc.plannerThreadsRun != 4):
            continue

        self.lock.acquire()
        while (self.queuesExecuted != 4):
            queueToExecute = self.quecc.setsOfQueues[self.queuesExecuted][self.transactionWorkerNum]
            while not (queueToExecute.empty()):
                query, args = queueToExecute.get()
                # print("executing at: ", self.queuesExecuted, " ", self.transactionWorkerNum)
                query(*args)
            self.queuesExecuted += 1
        self.lock.release()

        # self.result = 250
        # print("Result is being changed: ", self.result)
            # self.stats.append(True)
        # self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
