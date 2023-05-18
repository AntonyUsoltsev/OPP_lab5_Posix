#include <iostream>
#include <pthread.h>
#include <mpi.h>
#include <cmath>
#include <queue>

constexpr int RANK_ROOT = 0;
constexpr int SEND_TASK = 1;
constexpr int NEED_TASK = 2;

constexpr int ITER_COUNT = 10;
constexpr int TASK_ON_ITER = 10;

pthread_mutex_t mutex;
pthread_cond_t cond_queue_fill;
pthread_cond_t cond_queue_empty;

typedef struct {
    int repeat_num;
} task;


class taskQueue {
public:
    std::queue<task> queue;
    pthread_mutex_t queue_mutex;

    taskQueue() {
        pthread_mutex_init(&queue_mutex, nullptr);
    }

    ~taskQueue() {
        pthread_mutex_destroy(&queue_mutex);
    }

    void push(const task task_) {
        pthread_mutex_lock(&queue_mutex);
        queue.push(task_);
        pthread_mutex_unlock(&queue_mutex);
    }

    int pop() {
        pthread_mutex_lock(&queue_mutex);
        if (!queue.empty()) {
            int item = queue.front().repeat_num;
            queue.pop();
            pthread_mutex_unlock(&queue_mutex);
            return item;
        } else {
            pthread_mutex_unlock(&queue_mutex);
            return -1;
        }
    }

    bool is_empty() {
        pthread_mutex_lock(&queue_mutex);
        bool is_empty = queue.empty();
        pthread_mutex_unlock(&queue_mutex);
        return is_empty;
    }

    void print_queue() {
        pthread_mutex_lock(&queue_mutex);
        std::queue<task> copy_q = queue;
        while (!copy_q.empty()) {
            int value = copy_q.front().repeat_num;
            copy_q.pop();
            std::cout << value << " ";
        }
        std::cout << std::endl;
        pthread_mutex_unlock(&queue_mutex);
    };
};

typedef struct {
    bool ITER_CONTINUE;
    int CUR_ITER;
    int RANK;
    int SIZE;
    taskQueue task_queue;
} Context;


void *sender_thread(void *arg) {
    auto *context = (Context *) arg;
    while (context->CUR_ITER != ITER_COUNT && context->SIZE != 1) {
        MPI_Status status;
        MPI_Recv(nullptr, 0, MPI_INT, MPI_ANY_SOURCE, NEED_TASK, MPI_COMM_WORLD, &status);
        int sender = status.MPI_SOURCE;
        int rep_num = context->task_queue.pop();
        std::cout << "send rep num " << rep_num << std::endl;
        MPI_Send(&rep_num, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);

//        if (context->task_queue.queue.empty()) {
//            int notask = -1;
//            MPI_Send(&notask, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);
//        } else {
//            int rep_num = context->task_queue.pop();
//            std::cout << "send rep num " << rep_num<<std::endl;
//            MPI_Send(&rep_num, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);
//        }
    }
    pthread_exit(nullptr);
}

void *reciver_thread(void *arg) {
    auto *context = (Context *) arg;

    while (context->CUR_ITER != ITER_COUNT) {
//        if (context->THREAD_SHUTDOWN) {
//            pthread_exit(nullptr);
//        }

        pthread_mutex_lock(&mutex);
        while (!context->task_queue.is_empty() || !context->ITER_CONTINUE) {
            pthread_cond_wait(&cond_queue_empty, &mutex);
        }
        pthread_mutex_unlock(&mutex);

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

            context->task_queue.push({.repeat_num = recv_repeat_num});

        }

        if (context->task_queue.is_empty()) {
            pthread_mutex_lock(&mutex);
            context->ITER_CONTINUE = false;
            pthread_mutex_unlock(&mutex);
        }
        pthread_mutex_lock(&mutex);
        pthread_cond_signal(&cond_queue_fill);
        pthread_mutex_unlock(&mutex);
    }
    pthread_exit(nullptr);
}

void *executor_thread(void *arg) {
    auto *context = (Context *) arg;
    double global_res = 0;

    for (int i = 0; i < ITER_COUNT; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        int task_solved = 0;
        for (int j = 0; j < TASK_ON_ITER; ++j) {
            context->task_queue.push({.repeat_num = (context->RANK + 1) * (j + 1)});
            //  std::cout << context->task_queue.queue.back().repeat_num << " ";
        }

        pthread_mutex_lock(&mutex);
        context->ITER_CONTINUE = true;
        pthread_mutex_unlock(&mutex);
        while (true) {
            pthread_mutex_lock(&mutex);
            while (context->task_queue.is_empty() && context->ITER_CONTINUE) {
                pthread_cond_wait(&cond_queue_fill, &mutex);
            }
            pthread_mutex_unlock(&mutex);
            if (!context->ITER_CONTINUE) {
                break;
            }
           // context->task_queue.print_queue();
            int one_task_iter = context->task_queue.pop();

            double res = 0;
            for (int j = 0; j < one_task_iter; ++j) {
                res += sqrt(j);
            }
            task_solved++;
            //   std::cout << "Rank: " << context->RANK << " i:" << i << " rep num " << one_task.repeat_num << " res: "<< res << "\n";
            pthread_mutex_lock(&mutex);
            pthread_cond_signal(&cond_queue_empty);
            pthread_mutex_unlock(&mutex);
        }
        context->CUR_ITER++;
        MPI_Allreduce(&task_solved, &task_solved, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
        if(context->RANK == RANK_ROOT) {
            std::cout << "Task solved:" << task_solved << " on iter:" << i << std::endl;
        }

    }
    std::cout << "iter end\n";
    fflush(stdout);
//    pthread_mutex_lock(&mutex);
//    context->THREAD_SHUTDOWN = true;
//    pthread_mutex_unlock(&mutex);
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
            .ITER_CONTINUE = true,
            .CUR_ITER = 1,
            .RANK = rank,
            .SIZE = size
    };
    pthread_mutex_init(&mutex, nullptr);
    pthread_condattr_t condattrs;

    pthread_condattr_init(&condattrs);
    pthread_cond_init(&cond_queue_empty, &condattrs);
    pthread_cond_init(&cond_queue_fill, &condattrs);
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
    // pthread_cancel(send_thread);
    // pthread_cancel(recv_thread);
    std::cout << "All_end\n";
    fflush(stdout);
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond_queue_empty);
    pthread_cond_destroy(&cond_queue_fill);

    return 0;
}

