import time
from Neo4JWrapper import Neo4JWrapper
from multiprocessing import Pool
# from joblib import Parallel, delayed
from concurrent.futures import ThreadPoolExecutor


class GraphAnalytics:

    def __init__(self, databaseFile, d=0.85, iterations=100, threads=10):
        self.databaseFile = databaseFile
        self.indegree = {}
        self.outdegree = {}
        self.inedges = {}
        self.vertices = 0
        self.edges = 0
        self.is_triple = 0
        self.pr = None
        self.pr_old = None
        self.pr_new = None
        self.d = d
        self.iterations = iterations
        self.threads = threads

    def computedegrees(self):
        f = open(self.databaseFile + ".txt", "r")
        self.vertices, self.edges = f.readline().strip().split(" ")
        self.vertices = int(self.vertices)+1
        self.edges = int(self.edges)

        start = time.time()
        for line in f:
            line = line.strip()
            s, o = line.split(" ")

            s = int(s)
            o = int(o)
            if s not in self.outdegree:
                self.outdegree[s] = 1
            else:
                self.outdegree[s] += 1

            if o not in self.indegree:
                self.indegree[o] = 1
            else:
                self.indegree[o] += 1

            if o not in self.inedges:
                self.inedges[o] = [s]
            else:
                self.inedges[o].append(s)

        end = time.time()

        #print("Time taken to calculate degrees (ms): ", (end - start) * 100)

    def pagerank(self):
        d = self.d
        iterations = self.iterations
        pr_old = [1.0 / self.vertices for i in range(self.vertices)]
        pr_new = pr_old.copy()
        start = time.time()
        for iter in range(iterations):
            for i in range(self.vertices):
                sum = 0
                if i not in self.inedges:
                    continue
                for j in range(len(self.inedges[i])):
                    v = self.inedges[i][j]
                    try:
                        pr_old[v]
                    except Exception:
                        print (v, len(pr_old))
                        exit(0)
                    sum += float(pr_old[v]) / self.outdegree[v]

                pr_new[i] = (1.0 - d) / self.vertices + d * sum

            pr_old = pr_new.copy()
        end = time.time()

        print("Time taken pagerank (ms) : ", (end - start) * 100)
        self.pr = pr_new

    def node_parallel_pagerank(self, node_id):
        d = self.d
        sum = 0
        if node_id not in self.inedges:
            return
        for j in range(len(self.inedges[node_id])):
            v = self.inedges[node_id][j]
            sum += float(self.pr_old[v]) / self.outdegree[v]

        self.pr_new[node_id] = (1.0 - d) / self.vertices + d * sum

    def pagerank_parallel_threads(self):

        d = self.d
        threads = self.threads
        iterations = self.iterations

        self.computedegrees()
        self.pr_old = [1.0 / self.vertices for i in range(self.vertices)]
        self.pr_new = self.pr_old.copy()

        start = time.time()
        executor = ThreadPoolExecutor(max_workers=threads)
        # pool = Pool(threads)
        vertices = [i for i in range(self.vertices)]
        for iter in range(iterations):
            # result = pool.map(self.node_parallel_pagerank, vertices)
            result = executor.map(self.node_parallel_pagerank, vertices)

            self.pr_old = self.pr_new.copy()
        end = time.time()

        print("Time taken pagerank (ms) : ", (end - start) * 100)
        self.pr = self.pr_new

    def pagerank_parallel_process(self):

        d = self.d
        threads = self.threads
        iterations = self.iterations

        self.computedegrees()
        self.pr_old = [1.0 / self.vertices for i in range(self.vertices)]
        self.pr_new = self.pr_old.copy()

        start = time.time()
        # executor = ThreadPoolExecutor(max_workers=threads)
        pool = Pool(threads)
        vertices = [i for i in range(self.vertices)]
        for iter in range(iterations):
            result = pool.map(self.node_parallel_pagerank, vertices)
            # result = executor.map(self.node_parallel_pagerank, vertices)

            self.pr_old = self.pr_new.copy()
        end = time.time()

        print("Time taken pagerank (ms) : ", (end - start) * 100)
        self.pr = self.pr_new

    def pagerank_neo4j(self):
        iterations = self.iterations
        d = self.d
        neo = Neo4JWrapper("neo4j", "password")
        prs = neo.pagerank(d=d, N=self.vertices, iterations=iterations)
        # print(prs[0])

    def pagerank_neo4j_parallel(self):
        iterations = self.iterations
        d = self.d
        threads = self.threads
        neo = Neo4JWrapper("neo4j", "password")
        prs = neo.pagerank_parallel(d=d, N=self.vertices, iterations=iterations, threads=threads)


def main():
    graph = GraphAnalytics("Datasets/HM", iterations=10, threads=8)
    graph.computedegrees()
    print("Vertices : ", graph.vertices)
    print("Edges : ", graph.edges)
    # print("Serial pagerank")
    # graph.pagerank()
    # print("Pagerank parallel with threads")
    # graph.pagerank_parallel_threads()
    # print("Pagerank parallel with processes")
    # graph.pagerank_parallel_process()
    #print("Pagerank neo4j serial")
    #graph.pagerank_neo4j()
    print("Pagerank neo4j parallel")
    graph.pagerank_neo4j_parallel()
    # # print(graph.pr)
    # graph.pagerank_neo4j(iterations=1)
    # print(graph.pr[0])
    # print("Vertices : ", graph.vertices)
    # print("Edges : ", graph.edges)
    # # print ("Indegree : ", graph.indegree)
    # # print ("Outdegree : ", graph.outdegree)
    # # print ("Incoming edges : ", graph.inedges)
    # # print ("PageRank : ", graph.pr)



if __name__ == '__main__':
    main()
