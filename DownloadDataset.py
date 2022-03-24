import neo4j
class DownloadDataset:

    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        self.driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=(self.user, self.password))

    def downloadDataset(self):
        databaseName = self.database

        f = open (databaseName + ".txt", "w+")

        with self.driver.session() as session:
            result = session.run("MATCH (s)-[p]->(o) return id(s) as ids, id(p) as idp, id(o) as ido")
            ctr = 0
            for row in result:
                if ctr % 100000 == 0:
                    print ("Row : ", ctr)
                ctr+=1
                f.write(str(row["ids"]) + " " + str(row["idp"]) + " " + str(row["ido"]) + "\n")
        f.close()

def main():
    data = DownloadDataset("H&M", "neo4j", "pass")
    data.downloadDataset()

if __name__ == "__main__":
    main()

