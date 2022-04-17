#include<string.h>
#include<stdio.h>
#include<algorithm>
#include<queue>
#include<iostream>
#include<vector>
const double d = 0.85;
int V,E,L,M;

std::vector<std::vector<int>> in_edges;
std::vector<int> out_degree;

bool converged(std::vector<double> pr[2]){
	int N = pr[0].size();
	double sum = 0;
	for (int i = 0; i<N; ++i){
		sum += (pr[0][i] - pr[1][i])*(pr[0][i] - pr[1][i]);
	}

	//printf ("Sum : %0.8f", sum);

	if (sum<0.000000005)
		return true;
	else
		return false;
}

int main(int argc,char** argv){
	FILE* fin = fopen(argv[1],"r");
	//FILE* fout = fopen(argv[2],"w");
	fscanf(fin,"%d%d",&V,&E);
	in_edges.resize(V);
	out_degree = std::vector<int>(V,0);

	for(int i = 0;i < E;++i){
		int u,v;
		fscanf(fin,"%d%d",&u,&v);
		in_edges[v].push_back(u);
		++out_degree[u];
	}


	/*for (int i = 0; i<V; ++i){
		printf("\n");
		for (int j = 0; j<in_edges[i].size(); ++j){
			printf("%d ", in_edges[i][j]);
		}
	}*/
	std::vector<double> pr[2];
	pr[0].resize(V);
	pr[1].resize(V);
	int current = 0;
	//if (M>=1000)
	//	M = 1000;
	for(int i = 0;i < V;++i){
		pr[current][i] = 1.0 / V;
	}
	for(int iter = 0;iter < M;++iter){
		int next = 1 - current;
		//#pragma omp parallel shared(next, pr, in_edges, out_degree){
		#pragma omp parallel for shared(next, pr, in_edges, out_degree)
		for(int i = 0;i < V;++i){
			double sum = 0;
			#pragma omp parallel for reduction(+:sum)
			for(int j = 0;j < in_edges[i].size();++j){
				int v = in_edges[i][j];
				sum += pr[current][v] / out_degree[v];
				//printf("%d %f \n", v, sum);
			}
			pr[next][i] = (1.0 - d) / V + d * sum;
		}
		//}
		current = next;
		//printf("Iteration %d \n", iter);
	//	printf("\n");
	}

	fclose(fin);
	//fclose(fout);

	return 0;
}
