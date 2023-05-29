#include <iostream>
#include <pthread.h>
#include <mpi.h>
#include <cmath>
#include <queue>
#include <fstream>

constexpr int RANK_ROOT = 0;
constexpr int SEND_TASK = 1;
constexpr int NEED_TASK = 2;
constexpr int SHUTDOWN = -3;
constexpr int ALL_TASK_COUNT = 4096;

typedef struct {
    int repeat_num;
} task;

typedef struct {
    int RANK;
    int SIZE;
    int ITER_COUNT;
    int TASK_ON_ITER;

    std::queue<task> task_queue;
    std::ofstream out_file;

    pthread_mutex_t mutex;
} Context;


int pop(std::queue<task> &queue) {
    if (!queue.empty()) {
        int item = queue.front().repeat_num;
        queue.pop();
        return item;
    } else {
        return -1;
    }
}

void *executor_thread(void *arg) {
    auto *context = static_cast<Context *>(arg);

    for (int i = 0; i < context->ITER_COUNT; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        int task_solved = 0;
        pthread_mutex_lock(&context->mutex);
        for (int j = 0; j < context->TASK_ON_ITER; ++j) {
            context->task_queue.push({.repeat_num = (context->RANK * context->TASK_ON_ITER + j) * 10});
        }
        pthread_mutex_unlock(&context->mutex);

        while (true) {
            pthread_mutex_lock(&context->mutex);
            if (context->task_queue.empty()) {
                pthread_mutex_unlock(&context->mutex);
                break;
            }
            int one_task_iter = pop(context->task_queue);
            pthread_mutex_unlock(&context->mutex);
            double res = 0;
            for (int j = 0; j < one_task_iter; ++j) {
                res += sqrt(j);
            }
            task_solved++;
        }

    }
    pthread_exit(nullptr);
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

    Context context = {
            .RANK = rank,
            .SIZE = size,
            .ITER_COUNT = 30,
            .TASK_ON_ITER = ALL_TASK_COUNT / size
    };

    // std::string file = "./logs/info" + std::to_string(rank) + ".txt";
    //context.out_file.open(file, std::ios::out);

    pthread_mutex_init(&context.mutex, nullptr);

    pthread_attr_t attrs;
    pthread_t exec_thread;
    pthread_attr_init(&attrs);
    pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE);

    double start = MPI_Wtime();
    pthread_create(&exec_thread, &attrs, executor_thread, static_cast<void *>(&context));
    pthread_join(exec_thread, nullptr);
    double end = MPI_Wtime();

    if (rank == RANK_ROOT) {
        std::cout <<"Time: "<< end - start << std::endl;
    }
    pthread_attr_destroy(&attrs);
    pthread_mutex_destroy(&context.mutex);
    MPI_Finalize();

    return 0;
}
