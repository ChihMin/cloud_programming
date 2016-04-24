#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <math.h>
#define WORD 2500
#define LEN 50

char words[WORD][LEN];
int wt = 0;
double ans[WORD];
double max_err = 0.0;
double total_err = 0.0;

int main()
{
	FILE *fp_ans = fopen("ans.out", "r");
	while(~fscanf(fp_ans, "%s%lf", words[wt], &ans[wt])) {
		++wt;
	}
	fclose(fp_ans);
	int wc = 0;
	for (int rc = 0; rc < 2; ++ rc) {
		char filename[LEN];
		sprintf(filename, "part-r-%05d", rc);
		FILE *fp = fopen(filename, "r");
		char str[LEN];
		double tmp;
		while (~fscanf(fp, "%s%lf", str, &tmp)) {
			if (0 != strcmp(str, words[wc])) {
				printf("order mismatch (%d): %s %s\n", wc, str, words[wc]);
				exit(0);
			}
			double err = fabs(tmp - ans[wc]);
			max_err = max_err > err ? max_err : err;
			total_err += err;
			++wc;
		}
		fclose(fp);
	}
	if (wc != wt) {
		printf("word count mismatch\n");
		exit(0);
	}
	printf("  max err: %.15f\n", max_err);
	printf("total err: %.15f\n", total_err);
	return 0;
}