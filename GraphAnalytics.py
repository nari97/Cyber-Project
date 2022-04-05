
import time

class GraphAnalytics:

    def __init__(self, databaseFile):
        self.databaseFile = databaseFile
        self.indegree = {}
        self.outdegree = {}
        self.inedges = {}
        self.vertices = 0
        self.edges = 0
        self.is_triple = 0
        self.pr = None

    def computedegrees(self):
        f = open(self.databaseFile + ".txt", "r")
        self.vertices, self.edges = f.readline().strip().split(" ")
        self.vertices = int(self.vertices)
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

        print ("Time taken to calculate degrees (ms): ", (end - start)*100)

    def pagerank(self, iterations = 10000, d = 0.85):
        self.computedegrees()
        pr_old = [1.0/self.vertices for i in range(self.vertices)]
        pr_new = pr_old.copy()
        start = time.time()
        for iter in range(iterations):
            for i in range(self.vertices):
                sum = 0
                if i not in self.inedges:
                    continue
                for j in range(len(self.inedges[i])):
                    v = self.inedges[i][j]
                    sum += float(pr_old[v]) / self.outdegree[v]

                pr_new[i] = (1.0 - d)/self.vertices + d*sum

            pr_old = pr_new.copy()
        end = time.time()

        print ("Time taken pagerank (ms) : ", (end-start)*100)
        self.pr = pr_new



def main():
    graph = GraphAnalytics("Datasets/FB15K")
    graph.pagerank(iterations=2)

    print ("Vertices : ", graph.vertices)
    print ("Edges : ", graph.edges)
    #print ("Indegree : ", graph.indegree)
    #print ("Outdegree : ", graph.outdegree)
    #print ("Incoming edges : ", graph.inedges)
    #print ("PageRank : ", graph.pr)
if __name__ == '__main__':
    main()
    
