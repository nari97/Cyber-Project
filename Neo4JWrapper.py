import neo4j
import py2neo
import py2neo.bulk as bulk
from itertools import islice


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


if __name__ == "__main__":
    neo = Neo4JWrapper("neo4j", "password")
    neo.importDataset("YAGO")
