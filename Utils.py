def constructDataset(filename, dataset):
    vertices = int(open(filename + "\\entity2id.txt", encoding="utf8").readline())
    f_out = open("Datasets/" + dataset + ".txt", "w+")
    f1 = open(filename + "\\train2id.txt")
    f2 = open(filename + "\\valid2id.txt")
    f3 = open(filename + "\\test2id.txt")

    edges = int(f1.readline()) + int(f2.readline()) + int(f3.readline())

    f_out.write(str(vertices) + " " + str(edges) + "\n")

    for line in f1:
        splits = line.strip().split(" ")
        f_out.write(splits[0] + " " + splits[1] + "\n")

    for line in f2:
        splits = line.strip().split(" ")
        f_out.write(splits[0] + " " + splits[1] + "\n")

    for line in f3:
        splits = line.strip().split(" ")
        f_out.write(splits[0] + " " + splits[1] + "\n")

    f1.close()
    f2.close()
    f3.close()
    f_out.close()

if __name__ == "__main__":
    constructDataset(r"D:\PhD\Work\AugmentedKGE\AugmentedKGE\AugmentedKGE\Datasets\YAGO3-10", "YAGO")
