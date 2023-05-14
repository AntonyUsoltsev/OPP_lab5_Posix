#include <iostream>
#include <pthread.h>
#include <mpi.h>
#include <vector>

constexpr int RANK_ROOT = 0;
constexpr int FAIL = 0;
constexpr int SUCCESS = 1;
constexpr int REQUEST_TAG = 2;
constexpr int ANSWER_TAG = 3;
constexpr int NEED_TASKS = 4;
constexpr int TURN_OFF = 5;
constexpr int TASKS_IN_LIST = 200;
constexpr int L = 1000;
constexpr int ITERATION = 16;

pthread_mutex_t mutex;

typedef struct {
    int repeat_num;
} task;

void *sender_thread(void *arg) {
    auto *tasks = static_cast<std::vector<task> *>(arg);
//    for (int dest = 0; dest < tasks->size(); ++dest) {
//        if (dest != RANK_ROOT) {
//            MPI_Send(&((*tasks)[dest]), sizeof(task), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
//        }
//    }
    pthread_exit(nullptr);
}

void *reciver_thread(void *arg) {
    auto one_task = (task *) (arg);

//    for (int dest = 0; dest < tasks->size(); ++dest) {
//        if (dest != RANK_ROOT) {
//            MPI_Send(&((*tasks)[dest]), sizeof(task), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
//        }
//
//    }
    pthread_exit(nullptr);
}

void *executor_thread(void *arg) {
    auto one_task = *(task *) (arg);
    for (int i = 0; i < one_task.repeat_num; i++) {

    }
}



int main(int argc, char **argv) {
    int provided_roots;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided_roots);
    if (provided_roots != MPI_THREAD_MULTIPLE) {
        std::cerr << "Can't init MPI with MPI_THREAD_MULTIPLE level support" << std::endl;
        MPI_Finalize();
        return 0;
    }
    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    std::vector<task> Tasks;
    if (rank == RANK_ROOT) {
        Tasks.resize(size * 4);
        for (int i = 0; i < Tasks.size(); ++i) {
            Tasks[i].repeat_num = i + 1;
        }

        pthread_t send_thread;
        pthread_create(&send_thread, nullptr, sender_thread, static_cast<void *>(&Tasks));
    } else {
        task one_task;
        pthread_t recv_thread;
        pthread_create(&recv_thread, nullptr, reciver_thread, static_cast<void *>(&one_task));
    }



//    int result = pthread_create(&thread, nullptr, threadFunction, nullptr);
//    if (result != 0) {
//        std::cerr << "Error while creating thread" << std::endl;
//        return 1;
//    }
//    pthread_join(thread, nullptr);
    return 0;
}

