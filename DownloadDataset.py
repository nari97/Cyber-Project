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
        f.write(str(1478115) + " " + str(35483307) + "\n")
        with self.driver.session() as session:
            result = session.run("MATCH (s)-[]->(o) return id(s) as ids, id(o) as ido")
            ctr = 0
            for row in result:
                if ctr % 100000 == 0:
                    print ("Row : ", ctr)
                ctr+=1
                f.write(str(row["ids"]) + " " + str(row["ido"]) + "\n")
        f.close()

def main():
    data = DownloadDataset("HM", "neo4j", "pass")
    data.downloadDataset()

if __name__ == "__main__":
    main()

