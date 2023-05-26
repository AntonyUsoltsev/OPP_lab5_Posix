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
constexpr int ALL_TASK_COUNT = 1024;

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

void *sender_thread(void *arg) {
    auto *context = (Context *) arg;
    if (context->SIZE != 1) {
        while (true) {
            MPI_Status status;
            int recv_buff;
            MPI_Recv(&recv_buff, 1, MPI_INT, MPI_ANY_SOURCE, NEED_TASK, MPI_COMM_WORLD, &status);
            if (recv_buff == SHUTDOWN) {
                break;
            }
            int sender = status.MPI_SOURCE;
            pthread_mutex_lock(&context->mutex);
            int rep_num = pop(context->task_queue);
            pthread_mutex_unlock(&context->mutex);
            MPI_Send(&rep_num, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);
        }
    }
    pthread_exit(nullptr);
}


void *executor_thread(void *arg) {
    auto *context = static_cast<Context *>(arg);

    for (int i = 0; i < context->ITER_COUNT; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        int task_solved = 0;
        pthread_mutex_lock(&context->mutex);
        for (int j = 0; j < context->TASK_ON_ITER; ++j) {
            context->task_queue.push({.repeat_num = (context->RANK*context->TASK_ON_ITER + j) *10});
            //repeat_num = (context->RANK + 1) * 10 * (j + 1)
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
        while (true) {
            int recv_task_count = 0;
            for (int j = 0; j < context->SIZE; j++) {
                if (j == context->RANK) {
                    continue;
                }
                int send_buff = 0, recv_repeat_num = 0;
                MPI_Send(&send_buff, 1, MPI_INT, j, NEED_TASK, MPI_COMM_WORLD);
                MPI_Recv(&recv_repeat_num, 1, MPI_INT, j, SEND_TASK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                if (recv_repeat_num == -1) {
                    continue;
                }
                else {
                    recv_task_count++;
                    double res = 0;
                    for (int k = 0; k < recv_repeat_num; ++k) {
                        res += sqrt(k);
                    }
                    task_solved++;
                }
            }
            if (recv_task_count == 0) {
                break;
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
        context->out_file << "Task solved: " << task_solved << " on iter:" << i << " in process:" << context->RANK << std::endl;
    }

    MPI_Send(&SHUTDOWN, 1, MPI_INT, context->RANK, NEED_TASK, MPI_COMM_WORLD);
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
            .ITER_COUNT = 10,
            .TASK_ON_ITER = ALL_TASK_COUNT / size
    };

    std::string file = "info" + std::to_string(rank) + ".txt";
    context.out_file.open(file, std::ios::out);

    pthread_mutex_init(&context.mutex, nullptr);

    pthread_attr_t attrs;
    pthread_t send_thread, exec_thread;
    pthread_attr_init(&attrs);
    pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE);
    pthread_create(&exec_thread, &attrs, executor_thread, static_cast<void *>(&context));
    pthread_create(&send_thread, &attrs, sender_thread, static_cast<void *>(&context));
    pthread_attr_destroy(&attrs);

    pthread_join(exec_thread, nullptr);
    pthread_join(send_thread, nullptr);

    std::cout << "All_end in proc:" << context.RANK << std::endl;

    pthread_mutex_destroy(&context.mutex);
    MPI_Finalize();

    return 0;
}
