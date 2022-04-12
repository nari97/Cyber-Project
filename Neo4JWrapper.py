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

        # Dataset is assumed of form
        # Line 1: n_vertices n_entities
        # Line 2 onwards: head tail

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

        damping = (1.0 - d) / N
        start_pr = (1.0) / N
        graph = py2neo.Graph(self.uri, auth=(self.username, self.password))

        graph.run("match (e) set e.degree = size((e)-[]->()) return count(e)")
        graph.run("match (s) set s.o_pr = " + str(start_pr) + " ,s.n_pr = " + str(start_pr) + " return count(s)")
        print("Set o_pr, n_pr and degree")

        start = time.time()

        for iter in range(iterations):
            start_q = time.time()
            result = graph.run("match(o) with o match (s)-[]->(o) with s, o with o as o,(" + str(damping) + " + " + str(
                d) + "* sum(s.o_pr/s.degree)) as pr_sum set o.n_pr = pr_sum return count(o)")
            end_q = time.time()
            print("Query took : ", (end_q - start_q) * 100)
            graph.run("match (s) set s.o_pr = s.n_pr return count(s) as s")
        end = time.time()
        print("Time taken to calculate neo4j_pagerank (ms): ", (end - start) * 100)

        prs = {}
        result = graph.run("match (s) return s.inner_id as id, s.o_pr as pr")

        for row in result:
            prs[row["id"]] = row["pr"]
        return prs

    def pagerank_parallel(self, d, N, iterations, threads=100):
        damping = (1.0 - d) / N
        start_pr = (1.0) / N
        graph = py2neo.Graph(self.uri, auth=(self.username, self.password))

        graph.run("match (e) set e.degree = size((e)-[]->()) return count(e)")
        graph.run("match (s) set s.o_pr = " + str(start_pr) + " ,s.n_pr = " + str(start_pr) + " return count(s)")
        print("Set o_pr, n_pr and degree")

        inner_query = "optional match (s)-[]->(_) with s, _ as o with o as o," + str(damping) + "+" + str(
            d) + "* sum(s.o_pr/s.degree) as pr_sum return collect([o, pr_sum]) as out"

        outer_query = "match (e) with collect(e) as entities CALL apoc.cypher.mapParallel(\"" + inner_query + "\", {parallel: True, batchSize:100, concurrency:"+str(threads)+"}, entities) yield value unwind value.out as iter with iter[0] as node, iter[1] as pr set node.o_pr = pr return count(node)"

        start = time.time()
        for iter in range(iterations):
            print("Iteration: ", iter)
            result = graph.run(outer_query)

        end = time.time()

        print("Time taken to calculate neo4j_pagerank (ms): ", (end - start) * 100)

        prs = {}
        result = graph.run("match (s) return s.inner_id as id, s.o_pr as pr")

        for row in result:
            prs[row["id"]] = row["pr"]
        return prs





#if __name__ == "__main__":
#    neo = Neo4JWrapper("neo4j", "password")
    # neo.importDataset("YAGO")
#    neo.pagerank_parallel(0.85, 10, 1, threads=200)
