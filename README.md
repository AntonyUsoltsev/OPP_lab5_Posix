# OPP_lab5_Posix

Compile command:

    mpicxx main.cpp -o main -Wpedantic -Werror -Wall -O3 --std=c++11

Run command:

    mpirun -oversubscribe -np $proc_count ./main
