#include <iostream>
#include <pthread.h>
#include <mpi.h>
#include <cmath>
#include <queue>

constexpr int RANK_ROOT = 0;
constexpr int SEND_TASK = 1;
constexpr int NEED_TASK = 2;
constexpr int TURN_OFF = 3;

typedef struct {
    int repeat_num;
} task;

typedef struct {
    int ITER_COUNT;
    int TASK_ON_ITER;
    bool ITER_CONTINUE;
    bool THREAD_SHUTDOWN;
    int RANK;
    int SIZE;
    pthread_mutex_t mutex;
    pthread_cond_t cond_queue_fill;
    pthread_cond_t cond_queue_empty;
    std::queue<task> task_queue;
} Context;

void *sender_thread(void *arg) {
    auto *context = (Context *) arg;

    while (true) {
        if (context->THREAD_SHUTDOWN) {
            pthread_exit(nullptr);
        }
        MPI_Status status;
        MPI_Recv(nullptr, 0, MPI_INT, MPI_ANY_SOURCE, NEED_TASK, MPI_COMM_WORLD, &status);
     //   std::cout << " recv";
        int sender = status.MPI_SOURCE;
        if (context->task_queue.empty()) {
            int notask = -1;
            MPI_Send(&notask, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);
        } else {
            pthread_mutex_lock(&context->mutex);
            int rep_num = context->task_queue.front().repeat_num;
            context->task_queue.pop();
            pthread_mutex_unlock(&context->mutex);
            MPI_Send(&rep_num, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);
        }

    }
}

void *reciver_thread(void *arg) {
    auto *context = (Context *) arg;

    while (true) {
        if (context->THREAD_SHUTDOWN) {
            pthread_exit(nullptr);
        }

        pthread_mutex_lock(&context->mutex);
        while (!context->task_queue.empty() || !context->ITER_CONTINUE ) {
            pthread_cond_wait(&context->cond_queue_empty, &context->mutex);
        }
        pthread_mutex_unlock(&context->mutex);

        for (int i = 0; i < context->SIZE; i++) {
            if (i == context->RANK) {
                continue;
            }
            MPI_Send(nullptr, 0, MPI_INT, i, NEED_TASK, MPI_COMM_WORLD);
        }

        for (int i = 0; i < context->SIZE; i++) {
            if (i == context->RANK) {
                continue;
            }
            int recv_repeat_num;
            MPI_Recv(&recv_repeat_num, 1, MPI_INT, i, SEND_TASK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (recv_repeat_num == -1) {
                continue;
            }
            pthread_mutex_lock(&context->mutex);
            context->task_queue.push({.repeat_num = recv_repeat_num});
            pthread_mutex_unlock(&context->mutex);
        }
        if (context->task_queue.empty()) {
            pthread_mutex_lock(&context->mutex);
            context->ITER_CONTINUE = false;
            pthread_mutex_unlock(&context->mutex);
        }
        pthread_mutex_lock(&context->mutex);
        pthread_cond_signal(&context->cond_queue_fill);
        pthread_mutex_unlock(&context->mutex);

    }
}

void *executor_thread(void *arg) {
    auto *context = (Context *) arg;
    for (int i = 0; i < context->ITER_COUNT; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        std::cout<<"I: " << i << '\n';
        pthread_mutex_lock(&context->mutex);
        for (int j = 0; j < context->TASK_ON_ITER; ++j) {
            context->task_queue.push({.repeat_num = (context->RANK + 1) * (j + 1)});
            std::cout << context->task_queue.back().repeat_num << " ";
        }
        pthread_mutex_unlock(&context->mutex);

        pthread_mutex_lock(&context->mutex);
        context->ITER_CONTINUE = true;
        pthread_mutex_unlock(&context->mutex);
        while (true) {
//            if (!context->ITER_CONTINUE) {
//                break;
//            }
       //     std::cout<<"iter ";
            pthread_mutex_lock(&context->mutex);
            while (context->task_queue.empty() && context->ITER_CONTINUE) {
                //   std::cout<< "empty\n";
                pthread_cond_wait(&context->cond_queue_fill, &context->mutex);
            }
            pthread_mutex_unlock(&context->mutex);
            //   std::cout<<"aft empty\n";
            if (!context->ITER_CONTINUE) {
                break;
            }
            pthread_mutex_lock(&context->mutex);
            task one_task = context->task_queue.front();
            context->task_queue.pop();
            pthread_mutex_unlock(&context->mutex);

            double res = 0;
            for (int j = 0; j < one_task.repeat_num; ++j) {
                res += sqrt(j);
            }
            std::cout << res << " ";

            pthread_mutex_lock(&context->mutex);
            pthread_cond_signal(&context->cond_queue_empty);
            pthread_mutex_unlock(&context->mutex);
        }
        std::cout << "aft while "<<i<<"\n";

    }

    std::cout<<"iter end";
    pthread_mutex_lock(&context->mutex);
    context->THREAD_SHUTDOWN = true;
    pthread_mutex_unlock(&context->mutex);
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
    pthread_t send_thread, recv_thread, exec_thread;
    Context context = {
            .ITER_COUNT = 15,
            .TASK_ON_ITER = 10,
            .ITER_CONTINUE = true,
            .RANK = rank,
            .SIZE = size
    };
    pthread_mutex_init(&context.mutex, nullptr);
    pthread_condattr_t condattrs;

    pthread_condattr_init(&condattrs);
    pthread_cond_init(&context.cond_queue_empty, &condattrs);
    pthread_cond_init(&context.cond_queue_fill, &condattrs);
    pthread_condattr_destroy(&condattrs);

    pthread_attr_t attrs;
    pthread_attr_init(&attrs);
    pthread_attr_setdetachstate(&attrs, PTHREAD_CREATE_JOINABLE);
    pthread_create(&exec_thread, &attrs, executor_thread, static_cast<void *>(&context));
    pthread_create(&recv_thread, &attrs, reciver_thread, static_cast<void *>(&context));
    pthread_create(&send_thread, &attrs, sender_thread, static_cast<void *>(&context));
    pthread_attr_destroy(&attrs);

    pthread_join(exec_thread, nullptr);
    pthread_join(recv_thread, nullptr);
    pthread_join(send_thread, nullptr);
    std::cout << "end";
    pthread_mutex_destroy(&context.mutex);
    pthread_cond_destroy(&context.cond_queue_empty);
    pthread_cond_destroy(&context.cond_queue_fill);

    return 0;
}

