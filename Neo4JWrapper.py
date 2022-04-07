import neo4j
import py2neo
import py2neo.bulk as bulk
from itertools import islice
import time


class Neo4JWrapper:

    def __init__(self, username, password):
        self.uri = "bolt://localhost:7687"
        self.username = username
        self.password = password

    def batch(self, iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    def importDataset(self, dataset):

        #Dataset is assumed of form
        #Line 1: n_vertices n_entities
        #Line 2 onwards: head tail

        datasetFile = open("Datasets/" + dataset + ".txt")

        vertices, edges = datasetFile.readline().strip().split(" ")

        graph = py2neo.Graph(self.uri, auth=(self.username, self.password))
        # print ("Graph connection initialised : " + graph.name)
        keys = ["inner_id"]
        vertexNodes = []
        edgeList = []
        print("Creating entities")
        ctr = 1

        for i in range(int(vertices)):
            vertexNodes.append([i])

        batch_size = 10000

        for batch in self.batch(vertexNodes, batch_size):
            print("Batch : " + str(ctr))
            ctr += 1
            bulk.create_nodes(graph.auto(), batch, labels={"Entities"}, keys=keys)

        vertexDict = {}
        result = graph.run("MATCH (e) return id(e) as id, e.inner_id as inner")

        for line in result:
            vertexDict[int(line["inner"])] = int(line["id"])
        print("Created entities")

        print("Creating relationships")
        ctr = 1
        edgeList = []
        for line in datasetFile:
            splits = line.strip().split(" ")
            edgeList.append((vertexDict[int(splits[0])], {}, vertexDict[int(splits[1])]))

        for batch in self.batch(edgeList, batch_size):
            print("Batch : " + str(ctr))
            ctr += 1
            bulk.create_relationships(graph.auto(), batch, "CONNECTED")

        print("Created relationships")

    def pagerank(self, d, N, iterations):

        damping = (1.0-d)/N
        start_pr = (1.0)/N
        graph = py2neo.Graph(self.uri, auth=(self.username, self.password))

        graph.run("match (e) set e.degree = size((e)-[]->()) return count(e)")
        graph.run("match (s) set s.o_pr = " + str(start_pr) + " ,s.n_pr = " + str(start_pr) + " return count(s)")
        print("Set o_pr, n_pr and degree")

        start = time.time()
        for iter in range(iterations):
            graph.run("match(o) with o match (s)-[]->(o) with s, o with o as o,(" + str(damping) + " + " + str(d) + "* sum(s.o_pr/s.degree)) as pr_sum set o.n_pr = pr_sum return count(o)")
            graph.run("match (s) set s.o_pr = s.n_pr return count(s) as s")
        end = time.time()
        print("Time taken to calculate neo4j_pagerank (ms): ", (end - start) * 100)

        prs = {}
        result = graph.run ("match (s) return s.inner_id as id, s.o_pr as pr")

        for row in result:
            prs[row["id"]] = row["pr"]
        return prs

#if __name__ == "__main__":
#    neo = Neo4JWrapper("neo4j", "password")
#    neo.importDataset("YAGO")
