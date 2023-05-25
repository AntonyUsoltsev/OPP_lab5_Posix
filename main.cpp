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


typedef struct {
    int repeat_num;
} task;


class taskQueue {
public:
    std::queue<task> queue;
 //   pthread_mutex_t queue_mutex;

    taskQueue() {
        //pthread_mutex_init(&queue_mutex, nullptr);
    }

    ~taskQueue() {
     //   pthread_mutex_destroy(&queue_mutex);
    }

    void push(const task task_,pthread_mutex_t *mutex) {
        pthread_mutex_lock(mutex);
        queue.push(task_);
        pthread_mutex_unlock(mutex);
    }

    int pop(pthread_mutex_t *mutex) {
       // pthread_mutex_lock(mutex);
        if (!queue.empty()) {
            int item = queue.front().repeat_num;
            queue.pop();
           // pthread_mutex_unlock(mutex);
            return item;
        } else {
          //  pthread_mutex_unlock(mutex);
            return -1;
        }
    }

    bool is_empty() {
   //     pthread_mutex_lock(&queue_mutex);
        bool is_empty = queue.empty();
    //    pthread_mutex_unlock(&queue_mutex);
        return is_empty;
    }
//
//    void print_queue() {
//        pthread_mutex_lock(&queue_mutex);
//        std::queue<task> copy_q = queue;
//        while (!copy_q.empty()) {
//            int value = copy_q.front().repeat_num;
//            copy_q.pop();
//            std::cout << value << " ";
//        }
//        std::cout << std::endl;
//        pthread_mutex_unlock(&queue_mutex);
//    };
};

typedef struct {
    bool ITER_CONTINUE;
    int CUR_ITER;
    int RANK;
    int SIZE;
    taskQueue task_queue;
    int ITER_COUNT = 10;
    int TASK_ON_ITER = 10;

    pthread_mutex_t mutex;
    pthread_cond_t cond_queue_fill;
    pthread_cond_t cond_queue_empty;
    std::ofstream out_file;
} Context;


void *sender_thread(void *arg) {
    auto *context = (Context *) arg;

    while (context->CUR_ITER != context->ITER_COUNT && context->SIZE != 1) {
        MPI_Status status;

//        pthread_mutex_lock(&context->mutex);
//        context->out_file << "Wait recv:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//        pthread_mutex_unlock(&context->mutex);

        int recv_buff;
        MPI_Recv(&recv_buff, 1, MPI_INT, MPI_ANY_SOURCE, NEED_TASK, MPI_COMM_WORLD, &status);
        if (recv_buff == SHUTDOWN) {
            pthread_exit(nullptr);
        }

//        pthread_mutex_lock(&context->mutex);
//        context->out_file << "do recv:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//        pthread_mutex_unlock(&context->mutex);

        int sender = status.MPI_SOURCE;
        pthread_mutex_lock(&context->mutex);
        int rep_num = context->task_queue.pop(&context->mutex);
        pthread_mutex_unlock(&context->mutex);
        std::cout << "send rep num " << rep_num << " from:" << context->RANK << " to:" << sender << std::endl;
        MPI_Send(&rep_num, 1, MPI_INT, sender, SEND_TASK, MPI_COMM_WORLD);

    }

    for (int i = 0; i < context->SIZE; i++) {
        if (i == context->RANK) {
            continue;
        }
//        pthread_mutex_lock(&context->mutex);
//        context->out_file << "Wait send:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//        pthread_mutex_unlock(&context->mutex);
        int shutdown = SHUTDOWN;

        MPI_Send(&shutdown, 1, MPI_INT, i, SEND_TASK, MPI_COMM_WORLD);

//        pthread_mutex_lock(&context->mutex);
//        context->out_file << "do send:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//        pthread_mutex_unlock(&context->mutex);
    }
    pthread_exit(nullptr);
}

void *reciver_thread(void *arg) {
    auto *context = (Context *) arg;
    int recv_task_count=0;
    while (context->CUR_ITER != context->ITER_COUNT) {
        pthread_mutex_lock(&context->mutex);
        while (!context->task_queue.is_empty() || !context->ITER_CONTINUE) {
            pthread_cond_wait(&context->cond_queue_empty, &context->mutex);
        }
        pthread_mutex_unlock(&context->mutex);

        for (int i = 0; i < context->SIZE; i++) {
            if (i == context->RANK) {
                continue;
            }

            int send_buff = 0;
            MPI_Send(&send_buff, 1, MPI_INT, i, NEED_TASK, MPI_COMM_WORLD);

            pthread_mutex_lock(&context->mutex);
            context->out_file << "do send:" << __FUNCTION__ << " " << __LINE__ << std::endl;
            pthread_mutex_unlock(&context->mutex);
        }
        recv_task_count = 0;
        for (int i = 0; i < context->SIZE; i++) {
            if (i == context->RANK) {
                continue;
            }
            int recv_repeat_num;

//            pthread_mutex_lock(&context->mutex);
//            context->out_file << "Wait recv:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//            pthread_mutex_unlock(&context->mutex);

            MPI_Recv(&recv_repeat_num, 1, MPI_INT, i, SEND_TASK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            pthread_mutex_lock(&context->mutex);
            context->out_file << "do recv:" << __FUNCTION__ << " " << __LINE__ << std::endl;
            pthread_mutex_unlock(&context->mutex);
            if (recv_repeat_num == -1) {
                continue;
            } else if (recv_repeat_num == SHUTDOWN) {
                pthread_exit(nullptr);
            }
            recv_task_count++;
            context->task_queue.push({.repeat_num = recv_repeat_num},&context->mutex);

        }

        if (recv_task_count == 0) {
            pthread_mutex_lock(&context->mutex);
            context->ITER_CONTINUE = false;
            pthread_mutex_unlock(&context->mutex);
        }
     //   pthread_mutex_lock(&context->mutex);
        pthread_cond_signal(&context->cond_queue_fill);
       // pthread_mutex_unlock(&context->mutex);
    }
    for (int i = 0; i < context->SIZE; i++) {
        if (i == context->RANK) {
            continue;
        }
//        pthread_mutex_lock(&context->mutex);
//        context->out_file << "Wait send:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//        pthread_mutex_unlock(&context->mutex);
        int shutdown = SHUTDOWN;

        MPI_Send(&shutdown, 1, MPI_INT, i, NEED_TASK, MPI_COMM_WORLD);

//        pthread_mutex_lock(&context->mutex);
//        context->out_file << "do send:" << __FUNCTION__ << " " << __LINE__ << std::endl;
//        pthread_mutex_unlock(&context->mutex);
    }
    pthread_exit(nullptr);
}

void *executor_thread(void *arg) {
    auto *context = (Context *) arg;
    double global_res = 0;

    for (int i = 0; i < context->ITER_COUNT; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        int task_solved = 0;
        for (int j = 0; j < context->TASK_ON_ITER; ++j) {
            context->task_queue.push({.repeat_num = (context->RANK + 1) * (j + 1)},&context->mutex);
            //  std::cout << context->task_queue.queue.back().repeat_num << " ";
        }

        pthread_mutex_lock(&context->mutex);
        context->ITER_CONTINUE = true;
        pthread_mutex_unlock(&context->mutex);
        while (true) {
            ///PROBLEM!

            pthread_mutex_lock(&context->mutex);
            if (context->task_queue.is_empty() && context->ITER_CONTINUE) {
                context->out_file << "pthread_cond_wait:" << __FUNCTION__ << " " << __LINE__ << " on iter:" << i << std::endl;
                pthread_cond_wait(&context->cond_queue_fill, &context->mutex);
                context->out_file << "after pthread_cond_wait:" << __FUNCTION__ << " " << __LINE__ << " on iter:" << i<< " iter-cont:" << context->ITER_CONTINUE <<" is-empty:"<<context->task_queue.is_empty() << std::endl;
            }
            pthread_mutex_unlock(&context->mutex);
            pthread_mutex_lock(&context->mutex);
            if (!context->ITER_CONTINUE) {
                pthread_mutex_unlock(&context->mutex);
                break;
            }
            pthread_mutex_unlock(&context->mutex);
            // context->task_queue.print_queue();
            pthread_mutex_lock(&context->mutex);
            int one_task_iter = context->task_queue.pop(&context->mutex);
            pthread_mutex_unlock(&context->mutex);

            double res = 0;
            for (int j = 0; j < one_task_iter; ++j) {
                res += sqrt(j);
            }
            task_solved++;
            pthread_mutex_lock(&context->mutex);
            context->out_file << " i:" << i << " rep num " << one_task_iter << " res: " << res << std::endl;
            pthread_mutex_unlock(&context->mutex);
            // std::cout << "Rank: " << context->RANK << " i:" << i << " rep num " << one_task_iter << " res: "<< res << "\n";
            ///
            pthread_mutex_lock(&context->mutex);
            pthread_cond_signal(&context->cond_queue_empty);
            pthread_mutex_unlock(&context->mutex);

        }
        MPI_Barrier(MPI_COMM_WORLD);
        pthread_mutex_lock(&context->mutex);
        context->out_file << "Mutex lock:" << __FUNCTION__ << " " << __LINE__ << " on iter:" << i << std::endl;
        context->CUR_ITER++;
        pthread_mutex_unlock(&context->mutex);
        context->out_file << "Mutex unlock: " << __FUNCTION__ << " " << __LINE__ << " on iter:" << i << std::endl;

        // MPI_Allreduce(&task_solved, &task_solved, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

        std::cout << "Task solved: " << task_solved << " on iter:" << i << " in process:" << context->RANK << std::endl;


    }
    std::cout << "iter end in proc" << context->RANK << std::endl;
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
    if (rank == RANK_ROOT) {
        context.out_file.open("info0.txt", std::ios::out);
    } else if (rank == 1) {
        context.out_file.open("info1.txt", std::ios::out);
    } else {
        context.out_file.open("infoelse.txt", std::ios::out);
    }
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
    // pthread_cancel(send_thread);
    // pthread_cancel(recv_thread);
    std::cout << "All_end in proc" << context.RANK << std::endl;
    pthread_mutex_destroy(&context.mutex);
    pthread_cond_destroy(&context.cond_queue_empty);
    pthread_cond_destroy(&context.cond_queue_fill);
    MPI_Finalize();

    return 0;
}

// __FUNCTION__
// __LINE__