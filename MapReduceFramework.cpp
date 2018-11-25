
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <array>
#include <map>
#include <algorithm>
#include <semaphore.h>
#include <iostream>


std::atomic<int>* atomic_counter;
int numThreads=0;
pthread_mutex_t *sortMut;
pthread_cond_t *cv;
int ndone;
std::vector<IntermediateVec> readyToReduce;
sem_t mySemaphore;
pthread_mutex_t *reduceMutex;
pthread_mutex_t *outputMutex;
bool stillShuffling = true;


struct mapStruct{
    const MapReduceClient& ourClient;
    const InputVec& input;
    std::map<pthread_t , IntermediateVec> container;
    OutputVec outputVec;
};


void errorHandler(std::string functionName)
{
    std::cerr << "MapReduceFramework Failure: " + functionName + " failed." << std::endl;
    exit(1);
}


void checkFailure(int returnVal, std::string functionName)
{
    if(returnVal != 0)
    {
        errorHandler(functionName);
    }
}




void emit2 (K2* key, V2* value, void* context)
{
    IntermediatePair currPair = std::make_pair(key, value);
    IntermediateVec* myContainer = (IntermediateVec*) context;
    myContainer->push_back(currPair);
}


void emit3 (K3* key, V3* value, void* context)
{
    OutputPair currPair = std::make_pair(key, value);
    OutputVec* myOutput = (OutputVec*) context;
    checkFailure(pthread_mutex_lock(outputMutex), "pthread_mutex_lock");
    myOutput->push_back(currPair);
    checkFailure(pthread_mutex_unlock(outputMutex), "pthread_mutex_unlock");

}





class comparePair2
{
public:
    bool operator()(const IntermediatePair &pair1, const IntermediatePair &pair2)
    {
        return *pair1.first < *pair2.first;
    }
};




void* reducing(void* pVoid)
{
    mapStruct* myMap = (mapStruct*) pVoid;
    IntermediateVec currVec;
    while(stillShuffling || !readyToReduce.empty()) {
        checkFailure(sem_wait(&mySemaphore), "sem_wait");
        if (readyToReduce.empty())
        {
            break;
        }
        checkFailure(pthread_mutex_lock(reduceMutex), "pthread_mutex_lock");
        currVec = readyToReduce.back();
        readyToReduce.pop_back();
        checkFailure(pthread_mutex_unlock(reduceMutex), "pthread_mutex_unlock");
        myMap->ourClient.reduce(&currVec, (void *) &myMap->outputVec);
    }
    return nullptr;
}

void * mapping(void *pVoid)
{

    mapStruct* myMap = (mapStruct*) pVoid;
    IntermediateVec* currVec = new IntermediateVec;
    myMap->container[pthread_self()] = *currVec;
    unsigned int old_value = atomic_counter->load();
    while (old_value < myMap->input.size())
    {
        myMap->ourClient.map(myMap->input[old_value].first, myMap->input[old_value].second, (void*) currVec);
        old_value = *atomic_counter++;
    }
    pthread_exit(NULL);
}

K2* findMax(mapStruct* myMap)
{
    K2* maxVal = (*myMap->container.begin()).second.back().first;
    for (auto const& mapPair: myMap->container)
    {
        if(*maxVal < *mapPair.second.back().first)
        {
            maxVal = mapPair.second.back().first;
        }
    }
    return maxVal;
}







void* shuffle(void *pVoid) {
    mapStruct *myMap = (mapStruct *) pVoid;
    while (!myMap->container.empty()) {
//        IntermediatePair tempVal ;
        IntermediateVec currMaxKey;
        K2 *maxVal = findMax(myMap);
        auto iter = myMap->container.begin();
        while (iter != myMap->container.end()) {
            IntermediateVec ourVec = (*iter).second;
            while (!(*ourVec.back().first < *maxVal)) {
                auto tempVal = ourVec.back();
                ourVec.pop_back();
                currMaxKey.push_back(tempVal);
            }
            if (ourVec.empty()) {
                myMap->container.erase((*iter).first);
            } else {
                iter++;
            }
            checkFailure(pthread_mutex_lock(reduceMutex), "pthread_mutex_lock");
            readyToReduce.push_back(currMaxKey);
            checkFailure(sem_post(&mySemaphore), "sem_post");
            checkFailure(pthread_mutex_unlock(reduceMutex), "pthread_mutex_unlock");
        }
    }
    stillShuffling = false;
    for (int i = 0; i < numThreads; i++)
    {
        checkFailure(sem_post(&mySemaphore), "sem_post");
    }
    reducing((void *) myMap);
    pthread_exit(NULL);

}




void * sort(void *pVoid)
{
    mapStruct* myMap = (mapStruct*) pVoid;
    std::sort(myMap->container[pthread_self()].begin(), myMap->container[pthread_self()].end(), comparePair2());
    checkFailure(pthread_mutex_lock(sortMut), "pthread_mutex_lock");
    ndone += 1;
    if (ndone < numThreads)
    {
        checkFailure(pthread_cond_wait(cv, sortMut), "pthread_cond_wait");
    } else
    {
        checkFailure(pthread_cond_broadcast(cv), "pthread_cond_broadcast");
    }
    checkFailure(pthread_mutex_unlock(sortMut), "pthread_mutex_unlock");
    pthread_exit(NULL);
}


void runMapReduceFramework(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel)
{
    numThreads = multiThreadLevel;
    std::map<pthread_t , IntermediateVec> postMap;
    pthread_t* threads = new pthread_t[numThreads];
    atomic_counter = nullptr;

    mapStruct myMap = {client, inputVec, postMap, outputVec};
    for (int i = 0; i < multiThreadLevel; ++i) {
        checkFailure(pthread_create(threads + i, NULL, mapping, (void *) &myMap), "pthread_create");
    }
    for (int j = 0; j < multiThreadLevel; ++j) {
        checkFailure(pthread_join(threads[j], NULL), "pthread_join");
    }
    checkFailure(pthread_mutex_init(sortMut, NULL), "pthread_mutex_init");
    checkFailure(pthread_cond_init(cv, NULL), "pthread_cond_init");
    ndone = 0;

    for (int i = 0; i < multiThreadLevel; ++i) {
        checkFailure(pthread_create(threads + i, NULL, sort, (void *) &myMap), "pthread_create");
    }
    for (int j = 0; j < multiThreadLevel; ++j) {
        checkFailure(pthread_join(threads[j], NULL), "pthread_join");
    }
    checkFailure(pthread_mutex_destroy(sortMut), "pthread_mutex_destroy");
    checkFailure(sem_init(&mySemaphore,0,0), "sem_init");
    checkFailure(pthread_mutex_init(reduceMutex, NULL), "pthread_mutex_init");
    checkFailure(pthread_mutex_init(outputMutex, NULL), "pthread_mutex_init");
    checkFailure(pthread_create(threads, NULL, shuffle, (void *) &myMap), "pthread_create");
    for (int k = 1; k < multiThreadLevel; ++k) {
        checkFailure(pthread_create(threads + k, NULL, reducing, (void *) &myMap), "pthread_create");
    }
    for (int j = 0; j < multiThreadLevel; ++j) {
        checkFailure(pthread_join(threads[j], NULL), "pthread_join");
    }
    checkFailure(sem_destroy(&mySemaphore), "sem_destroy");
    checkFailure(pthread_mutex_destroy(reduceMutex), "pthread_mutex_destroy");
    checkFailure(pthread_mutex_destroy(outputMutex), "pthread_mutex_destroy");
    delete(threads);

}
