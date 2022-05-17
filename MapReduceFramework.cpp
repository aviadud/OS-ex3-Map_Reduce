/**
 * @file MapReduceFarmework.cpp
 * @author Elkana Tovey, Aviad Dudkevich
 *
 * @brief Implement of MapReduce framework and algorithm.
 */
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <semaphore.h>
#include "Barrier.h"
#include "MapReduceFramework.h"
#include <iostream>


typedef std::vector<IntermediateVec*> shuffleVector;
/**
 * assist every thread to do its work.
 */
struct ThreadContext{
    int threadID;
    int *sumOfIntermediatePairs;
    int numOfThreads;
    std::atomic<unsigned long>* atomic_counter;
    std::atomic<unsigned long>* atomic_mapping_counter;
    const InputVec *inputVec;
    IntermediateVec** intermediateThreadVec;

    shuffleVector *shuffleWork;

    const MapReduceClient *client;
    Barrier *barrier;
    sem_t *reduceSemaphore;
    pthread_mutex_t *reduceMutex;
    pthread_mutex_t *outputMutex;
    int stage = UNDEFINED_STAGE;
    OutputVec *output;
};

/**
 * used for JobHandle.
 */
struct JobInfo {
    int numberOfThreads;
    pthread_t *threadsList;
    ThreadContext *context;
    pthread_mutex_t *waitMutex;
    bool wait;
};

/**
 * lock a given mutex, print and exit if error occured.
 */
inline void mutexLockWithErrors(pthread_mutex_t *mutex)
{
    if (pthread_mutex_lock(mutex) != 0)
    {
        std::cerr << "[MapReduceFramework] error on pthread_mutex_lock" << std::endl;
        exit(1);
    }
}

/**
 * unlock a given mutex, print and exit if error occured.
 */
inline void mutexUnlockWithErrors(pthread_mutex_t *mutex)
{
    if (pthread_mutex_unlock(mutex) != 0)
    {
        std::cerr << "[MapReduceFramework] error on pthread_mutex_unlock" << std::endl;
        exit(1);
    }
}

/**
 * destroy a given mutex, print and exit if error occured.
 */
inline void mutexDestroyWithWarnings(pthread_mutex_t *Mutex)
{
    if (pthread_mutex_destroy(Mutex) != 0){
        std::cerr << "[MapReduceFramework] error on pthread_mutex_destroy" << std::endl;
        exit(1);
    }
}

/**
 * map every element in the input vector concurrently
 * @param tc thread context
 */
void mapInputVector(ThreadContext *tc)
{
    if(tc->threadID == 0)
    {
        tc->stage = 1;
    }
    unsigned long inputVectorLength = tc->inputVec->size();
    unsigned long oldValue = (*(tc->atomic_counter))++;
    while(oldValue < inputVectorLength)
    {
        //map first pair
        tc->client->map(tc->inputVec->at(oldValue).first, tc->inputVec->at(oldValue).second, tc);
        (*(tc->atomic_mapping_counter))++;
        //get more work
        oldValue = (*(tc->atomic_counter))++;
    }
}

/**
 * finds max key in set of vectors based upon context
 * @param tc context
 * @return maxKey or nullptr if all done
 */
K2 *getMaxKey(ThreadContext *tc)
{
    K2 *maxKey = nullptr;
    for(int i=0; i<tc->numOfThreads; ++i)
    {
        if(tc->intermediateThreadVec[i]->empty())
        {
            continue;
        }
        if(maxKey == nullptr|| *maxKey < *tc->intermediateThreadVec[i]->back().first)
        {
            maxKey = tc->intermediateThreadVec[i]->back().first;
        }
    }
    return maxKey;
}

/**
 * for sorting the intermediate vector.
 */
bool compare(IntermediatePair a, IntermediatePair b)
{
    return *a.first < *b.first;
}

/**
 * performs shuffle
 * @param tc the threadcontext of thread 0
 */
void shuffle(ThreadContext *tc)
{
        // for every key - from the grater to the smaller - create a intermediate vector that all
        // the pairs have the same key and push it to the queue.
        K2* maxKey = getMaxKey(tc);
        while(maxKey!= nullptr)
        {
            auto *vectorToReduce = new IntermediateVec;
            //insertion into vector with keys of same type to be summed
            for(int i=0; i<tc->numOfThreads; ++i)
            {
                //check if top of current equals max
                while(!tc->intermediateThreadVec[i]->empty() &&
                      (!(*tc->intermediateThreadVec[i]->back().first < *maxKey) &&
                       !(*maxKey < *tc->intermediateThreadVec[i]->back().first)))
                {
                    //pop and add to vector of single type keys,
                    //will be done with vector semaphore and queue
                    vectorToReduce->push_back(tc->intermediateThreadVec[i]->back());
                    tc->intermediateThreadVec[i]->pop_back();
                }
            }
            //enter found vector to handler and notify waiting threads
            mutexLockWithErrors(tc->reduceMutex);
            tc->shuffleWork->push_back(vectorToReduce);
            mutexUnlockWithErrors(tc->reduceMutex);
            if (sem_post(tc->reduceSemaphore) != 0)
            {
                fprintf(stderr, "[[Shuffle]] error on sem_post");
                exit(1);
            }
            maxKey = getMaxKey(tc);
        }
        //advance semaphore enough that everyoe can escape and exit
        for(int i=0; i<tc->numOfThreads; ++i)
        {
            if (sem_post(tc->reduceSemaphore) != 0)
            {
                fprintf(stderr, "[[Shuffle]] error on sem_post");
                exit(1);
            }
        }
}

/**
 * performs reduce
 * @param tc the threadcontext of the current thread.
 */
void reduce(ThreadContext *tc)
{
    while(true)
    {
        sem_wait(tc->reduceSemaphore);
        mutexLockWithErrors(tc->reduceMutex);
        if (tc->shuffleWork->empty())
        {
            //if we are here than n more work and escaping
            mutexUnlockWithErrors(tc->reduceMutex);
            return;
        }
        IntermediateVec *toReduce = tc->shuffleWork->back();
        tc->shuffleWork->pop_back();
        mutexUnlockWithErrors(tc->reduceMutex);
        tc->client->reduce(toReduce, tc);
        (*(tc->atomic_counter))+= toReduce->size();
        delete toReduce;
    }
}

/**
 * handles main logic for algorithm
 * @param arg - threadContext list
 */
void* startThreadWork(void *arg)
{
    auto * tc = (ThreadContext*) arg;
    mapInputVector(tc);

    //once here we finished mapping, and there no more elements to map - sort the intermediate
    // vector.
    std::sort(tc->intermediateThreadVec[tc->threadID]->begin(),
            tc->intermediateThreadVec[tc->threadID]->end(), compare);

    //finished sort and hit barrier
    tc->barrier->barrier();

    //condition for shuffle
    if(tc->threadID == 0) // insure only one thread will shuffle.
    {
        mutexLockWithErrors(tc->outputMutex);
        *(tc->atomic_counter) = 0;
        tc->sumOfIntermediatePairs = new int(0);
        for(int i=0; i<tc->numOfThreads; ++i) {*tc->sumOfIntermediatePairs += tc->intermediateThreadVec[i]->size();}
        tc->stage = 2;
        mutexUnlockWithErrors(tc->outputMutex);
        shuffle(tc);
    }
    //logic for reduce
    reduce(tc);

    return nullptr; // end the current thread.
}

/**
 * start a new MapReduce job.
 * @param client MapReduceClient
 * @param inputVec (key1, v1) vector of inputs
 * @param outputVec (key3, v3) empty vector of outputs
 * @param multiThreadLevel number of threads to generate
 * @return jobHandle
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    auto *threads = new pthread_t[multiThreadLevel];
    auto *contexts = new ThreadContext[multiThreadLevel];
    auto *reduceSem = new sem_t;
    auto *waitMutex = new pthread_mutex_t;
    auto *reduceMutex = new pthread_mutex_t();
    auto *outputMutex = new pthread_mutex_t();
    auto *shuffleWork = new shuffleVector;

    if (sem_init(reduceSem, 0, 0) != 0){
        std::cerr << "[MapReduceFramework] error on sem_init" << std::endl;
        exit(1);
    }
    if(pthread_mutex_init(reduceMutex, nullptr)!=0||pthread_mutex_init(waitMutex, nullptr)!=0||
       pthread_mutex_init(outputMutex, nullptr)!=0)
    {
        std::cerr << "[MapReduceFramework] error on mutex_init" << std::endl;
        exit(1);
    }

    auto *atomic_counter = new std::atomic<unsigned long>(0);
    auto *atomic_mapping_counter = new std::atomic<unsigned long>(0);
    auto **allIntermediates = new IntermediateVec*[multiThreadLevel];
    auto *barrier = new Barrier(multiThreadLevel);
    contexts[0].stage = UNDEFINED_STAGE;
    for(int i=0; i<multiThreadLevel; ++i)
    {
        contexts[i].threadID = i;
        contexts[i].numOfThreads = multiThreadLevel;
        contexts[i].atomic_counter = atomic_counter;
        contexts[i].atomic_mapping_counter = atomic_mapping_counter;
        contexts[i].inputVec = &inputVec;
        allIntermediates[i] = new IntermediateVec();
        contexts[i].intermediateThreadVec = allIntermediates;
        contexts[i].shuffleWork = shuffleWork;
        contexts[i].client = &client;
        contexts[i].barrier = barrier;
        contexts[i].reduceSemaphore = reduceSem;
        contexts[i].reduceMutex = reduceMutex;
        contexts[i].outputMutex = outputMutex;
        contexts[i].output = &outputVec;
    }
    for(int i=0; i<multiThreadLevel; ++i)
    {
        if (pthread_create(threads + i, nullptr, startThreadWork, contexts + i) != 0)
        {
            std::cerr << "[MapReduceFramework] error on pthread_create" << std::endl;
            exit(1);
        }
    }
    auto *jobInfo = new  JobInfo{multiThreadLevel, threads, contexts,
                                 waitMutex, false};
    return (JobHandle) jobInfo;
}

/**
 * waits until the job is finished.
 * @param job - JobHandle
 */
void waitForJob(JobHandle job)
{
    auto * jc = (JobInfo*) job;

    // insure only pne thread calls waitForJob:
    mutexLockWithErrors(jc->waitMutex);
    if (jc->wait){
        std::cerr << "[MapReduceFramework] error: two threads called waitForJob simultaneously"
        << std::endl;
        mutexUnlockWithErrors(jc->waitMutex);
        return;
    } else {
        jc->wait = true;
    }
    mutexUnlockWithErrors(jc->waitMutex);

    //wait until received jobhandle finished, received from startmapreducejob
    for(int i=0; i<jc->numberOfThreads; ++i)
    {
        if (pthread_join(jc->threadsList[i], nullptr) !=0){ //merges threads to main
            std::cerr << "[MapReduceFramework] error on pthread_join" << std::endl;
            exit(1);
        }
    }
    delete[] jc->threadsList;//The nullptr flag means that this needs to be deleted here
    jc->threadsList = nullptr;
}

/**
 * @param job - JobHandle
 * @param state - a pointer to JobState struct to check the given job state.
 */
void getJobState(JobHandle job, JobState *state)
{
    auto * jc = (JobInfo*) job;
    mutexLockWithErrors(jc->context[0].outputMutex);
    state->stage = (stage_t)jc->context[0].stage;
    if(jc->context[0].inputVec->empty()){state->percentage = 100; return;}
    if(jc->context[0].stage == MAP_STAGE)
    {
        state->percentage = 100 * (float)*jc->context[0].atomic_mapping_counter/
                                    jc->context[0].inputVec->size();
    }
    else if(jc->context[0].stage == REDUCE_STAGE)
    {
        state->percentage = 100 * (float) *jc->context[0].atomic_counter /
                                *jc->context[0].sumOfIntermediatePairs;

    }
    mutexUnlockWithErrors(jc->context[0].outputMutex);
}

/**
 * After the given job is done - releasing all the job resources.
 * @param job - JobHandle.
 */
void closeJobHandle(JobHandle job)
{
//releases jobs resources
    auto * jc = (JobInfo*) job;
    if(jc->threadsList != nullptr)//@todo should these finish right away, or wait?
    {
        waitForJob(jc);
    }
    for(int i=0; i<jc->numberOfThreads; ++i)
    {
        delete jc->context[i].intermediateThreadVec[i];
    }
    // needs to be deleted only once
    delete jc->context[0].sumOfIntermediatePairs;
    delete jc->context[0].shuffleWork;
    delete[] jc->context[0].intermediateThreadVec;
    delete jc->context[0].barrier;
    delete jc->context[0].atomic_counter;
    delete jc->context[0].atomic_mapping_counter;
    if (sem_destroy(jc->context[0].reduceSemaphore) != 0){
        std::cerr << "[MapReduceFramework] error on sem_destroy" << std::endl;
        exit(1);
    }
    delete jc->context[0].reduceSemaphore;

    mutexDestroyWithWarnings(jc->context[0].reduceMutex);
    delete jc->context[0].reduceMutex;

    mutexDestroyWithWarnings(jc->context[0].outputMutex);
    delete jc->context[0].outputMutex;

    delete[] jc->context;
    mutexDestroyWithWarnings(jc->waitMutex);
    delete jc->waitMutex;
    delete jc;
}

/**
 * called by client map function.
 * @param key - intermediate key.
 * @param value - intermediate value.
 * @param context - thread context.
 */
void emit2(K2 *key, V2 *value, void *context)
{
    auto * tc = (ThreadContext*) context;
    //join the key pair to intermediate.
    tc->intermediateThreadVec[tc->threadID]->push_back({key, value});
}

/**
 * called by client reduce function.
 * @param key - output key.
 * @param value - output value.
 * @param context - thread context.
 */
void emit3(K3 *key, V3 *value, void *context)
{
    auto * tc = (ThreadContext*) context;
    mutexLockWithErrors(tc->outputMutex);
    tc->output->push_back({key, value});
    mutexUnlockWithErrors(tc->outputMutex);
}



