#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <limits.h>
#include <time.h>
#include <sys/time.h>

#define START struct timeval* timeStart = startTime();
#define END endTime(timeStart);

// size of unsorted list
int SIZE; 
// number of threads/processors
int T;  

/*
 * store the original unsorted list and the final sorted list
 */
int* ARRAY;

/*
 * 
 * Variable that store the regular sample
 * It is generated in Phase 1, and used in Phase 2
 */
int* gatheredRegularSample;
/*
 *
 * It stores pivots gathered by main thread in Phase 2
 */
int* pivots;
/* 
 *
 * It is generated in Phase 3, it will store all starting and ending indexes of partitions 
 */
int* partitions;

int* mergedPartitionLength;

struct threadData {
	int tid;
	int start;
	int end;
};

pthread_barrier_t barrier;

int compare(const void* a, const void* b);
void isSorted();
struct timeval* startTime();
long int endTime(struct timeval* starttime);
int* generateData(int size);
void phase1(struct threadData* data);
void phase2(struct threadData* data);
void phase3(struct threadData* data);
void phase4(struct threadData* data);
int* findValidIndex(int * arr, int size);
void* psrs(void *args);

 
/*
 * value compare function used for qsort
 */
int compare (const void * a, const void * b) { return ( *(int *) a - *(int*)b );}


/*
 * verify whethter the result is correct or not
 */
void isSorted() {
	for (int i = 0; i < SIZE - 1; i++) {
		if (ARRAY[i] > ARRAY[i+1]) {
			printf("result is not a sorted list");
			return;	
		}
	}
	printf("Sorted\n");
}

/*
 * function used to get the exact start time of action
 */
struct timeval* startTime() {
	struct timeval* res = malloc(sizeof(struct timeval));
	gettimeofday(res, NULL);
	return res;
}

/*
 * function used to get the total time spent on the action
 */
long int endTime(struct timeval* starttime) {
	struct timeval endtime; 
	gettimeofday(&endtime, NULL);
	int multiple = 1000000;
	long int res = (long int) (( endtime.tv_usec + endtime.tv_sec * multiple) - (starttime->tv_usec + starttime->tv_sec * multiple));
	free(starttime);
	return res;
}

/*
 * generating testing data of given size
 */
int* generateData(int size) {
	srandom(12);
	int* res = malloc(sizeof(int) * size);
	for (int i = 0; i < size; i++) {
		res[i] = (int) random();
	}
	return res;
}

/*
 * Phase 1
 * each processor will do the local sorting of the original array and 
 * collect regular samples from their local sorting array
 */
void phase1(struct threadData* data) {
	START;

	int start = data->start;
	int end = data->end;
	int tid = data->tid;
	
	qsort((ARRAY + start), (end - start), sizeof(int), compare);
	
	// regular sample starts
	int index = 0;
	// parameter used for local indices
	int W = (SIZE/(T*T));
	for (int i = 0; i < T; i++) {
		gatheredRegularSample[tid * T + index] = ARRAY[start + (i * W)];
		index++;
	}
	
	long int time = END;
	printf("Thread %d : spent %ld ms, sorted %d items in phase 1\n", tid, time, (end - start));	
}

/*
 * Phase 2
 * Main thread will determine the pivots from the regular sample collected by each thread from the last phase
 */
void phase2(struct threadData* data) {	
	int tid = data->tid;
	if (tid == 0) { 
		START;
		// sorting the orginal gatehered regular samples
		qsort(gatheredRegularSample, T*T, sizeof(int), compare);
		int index = 0;
		for (int i = 1; i < T; i++) {
			int pos = T * i + (T / 2) - 1;
			// gather the result and put it into the global variable
			pivots[index++] = gatheredRegularSample[pos];
		}
		
		long int time = END;
		printf("Thread %d : spent %ld ms in phase 2\n", tid, time);
	}
	
}

/*
 * Phase 3
 * each processor will split the local block based on the pivots from the last phase to form p partitions
 */
void phase3(struct threadData* data) {
	START;

	int start = data->start;
	int end = data->end;
	int tid = data->tid;
	// pivot counter
	int vc = 0;
	// partition counter 
	int pc = 1;
	partitions[tid*(T+1)+0] = start;
	partitions[tid*(T+1)+T] = end;
	for (int i = start; i < end && vc != T-1; i++) {
		if (pivots[vc] < ARRAY[i]) {
			partitions[tid*(T+1) + pc] = i;
			vc++;
			pc++;
		}
	}

	
	long int time = END;
	printf("Thread %d : spent %ld ms in phase 3\n", tid, time);
}

/* 
 * This function will return the first valid index and value of the index in the assignedPartition.
 * The valid index stands for the index in the original list that the value of the index has not been put 
 * into the result-mergePartition of this phase.
 *  
 */
int* findValidIndex(int * arr, int size) {
	for (int i = 0; i < size; i += 2) {
		if (arr[i] != arr[i+1]) {
			int* valAndPos = malloc(sizeof(int) * 2);
			valAndPos[0] = ARRAY[arr[i]];
			valAndPos[1] = i;
			return valAndPos;
		}
	}
	return NULL;
}

/*
 * Phase 4
 * 
 * each processor will merge the partitions from the last phase into a local formed partition and 
 * then merge the formed partition into the original array to form a sorted array.
 */
void phase4(struct threadData* data) {
	START;
	
	int counter = 0;
	int tid = data->tid;
	// this array contains partitions starting and ending index
	// data in this will be:
	// [partition1_start, partition1_end, partition2_start, partition2_end, ...]
	int assignedPartition[T*2];
	

	 // each processor will get all assigned partition from partitions
	for (int i = 0; i < T; i++) {
		assignedPartition[counter++] = partitions[i*(T+1) + tid];
		assignedPartition[counter++] = partitions[i*(T+1) + tid + 1];
	}
	
	// the length of all assigned parttition the current processor received
	int totalPartitionLength = 0;
	for (int i = 0; i < T * 2; i+=2) {
		totalPartitionLength += assignedPartition[i + 1] - assignedPartition[i];
	}
	// store the final merged partition of this thread
	int* mergedPartition = malloc(sizeof(int) * totalPartitionLength);
	mergedPartitionLength[tid] = totalPartitionLength;

	// k-way merge start

	// mergedPartition index
	int mpIndex = 0;
	// do it until all the assigned partitions values have been put into the mergedPartition
	while (mpIndex < totalPartitionLength) {
		// use the first valid index we found in assignedPartition as the initial minimum value
		int* valAndPos = findValidIndex(assignedPartition, T * 2);
		if (valAndPos == NULL) break;
		int min = valAndPos[0];
		int minPos = valAndPos[1];
		
		// checking each assigned partition's starting index to find the minimum value and index on this step 
		for (int i = 0; i < T * 2; i+=2) {
			if (assignedPartition[i] != assignedPartition[i+1]) {
				int index = assignedPartition[i];
				if (ARRAY[index] < min) {
					min = ARRAY[index];
					minPos = i;
				}
			}
		}
		// save the minimum value to the mergedPartition
		mergedPartition[mpIndex++] = min;
		// update the valid index inside the assignedPartition
		assignedPartition[minPos]++;
		free(valAndPos);
	}
	// k-way merge finished

	// make sure all the thread/processor finished the k-way merge
	pthread_barrier_wait(&barrier);
	
	// merge local partition of each thread to the original array

	// find the starting index in the original array of each thread
	int startPosition = 0;
	int index = tid - 1;
	while (index >= 0) {
		startPosition += mergedPartitionLength[index--];
	}
	// merge into original array
	for (int i = startPosition; i < startPosition + totalPartitionLength; i++) {
		ARRAY[i] = mergedPartition[i - startPosition];
	}
	free(mergedPartition);

	long int time = END;
	printf("Thread %d - Phase 4 took %ld ms, merged %d keys\n", tid, time, totalPartitionLength);
}

/*
 * 4 phase psrs algorithm
 */
void* psrs(void *args) {
	struct threadData* data = (struct threadData*) args;
	int tid = data->tid; 

	// Phase 1 
	phase1(data);
	pthread_barrier_wait(&barrier);

	// Phase 2 
	phase2(data);
	pthread_barrier_wait(&barrier);
	if (tid == 0) { free(gatheredRegularSample); }

	// Phase 3 
	phase3(data);
	pthread_barrier_wait(&barrier);
	if (tid == 0) { free(pivots); }
	
	// Phase 4 
	phase4(data);
	pthread_barrier_wait(&barrier);
	if (tid == 0) { 
		free(mergedPartitionLength); 
		free(partitions);
	}

	free(data);	
	
	if (tid == 0) {
		return NULL;
	}
	pthread_exit(0);
}

struct threadData* divideDataForThread(int tid, int length) {
	struct threadData* data = malloc(sizeof(struct threadData));
	data->tid = tid;
	data->start = tid * length;
	data->end = data->start + length;
	return data;
}

/*
 * main function(thread)
 */
int main(int argc, char *argv[]){
	if (argc != 3) {
		fprintf(stderr, "2 arguments required - <SIZE> <THREAD_COUNT>\n");
		exit(1);
	} 
	
	// initializing parameters
	SIZE 	= atoi(argv[1]);
	T 	= atoi(argv[2]);
	
	printf("SIZE: %d\n", SIZE);
	
	// get unsorted list
	ARRAY = generateData(SIZE);
	// deal with the situation with only one processor
	if(T == 1){
		START;
		qsort(ARRAY, SIZE, sizeof(int), compare);
		long int time = END;
		printf("Total time taken is: %ld ms (microseconds)\n", time);
		return 0;
	}
	// initialize parameters
	gatheredRegularSample = malloc(sizeof(int) *T*T); 
	pivots 			= malloc(sizeof(int) * (T - 1));
	mergedPartitionLength 	= malloc(sizeof(int) * T);
	partitions 		= malloc(sizeof(int) *  T * (T+1));
	
	// size of a local block of original array for per thread
	int blockSize = SIZE / T;
	
	pthread_barrier_init(&barrier, NULL, T);
	
	pthread_t* THREADS = malloc(sizeof(pthread_t) * T);

	START;	
	
	int i;
	// create processors
	for (i = 1; i < T - 1; i++) {
		struct threadData* data = divideDataForThread(i, blockSize);
		pthread_create(&THREADS[i], NULL, psrs, (void *) data);
	}
	// deal with the situation that the size of orginal unsorted list is not evenly divisible by the number of processor
	struct threadData* data = divideDataForThread(i, blockSize); 
	data->end = SIZE; 
	// this thread/processor will deal with the remaining part of the original unsorted list
	pthread_create(&THREADS[i], NULL, psrs, (void *) data);
	// master thread will be also used to perform the psrs algorithm
	struct threadData* masterData = divideDataForThread(0, blockSize);
	psrs((void *) masterData);
	
	long int time = END;
	printf("Total time taken is: %ld ms (microseconds)\n", time);
	
 	isSorted(); // for validation to see if the array has really been sorted

	pthread_barrier_destroy(&barrier);
	free(THREADS);
	free(ARRAY);
		
	return 0;
}
