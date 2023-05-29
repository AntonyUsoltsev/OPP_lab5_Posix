# shellcheck disable=SC1073
mpicxx main.cpp -o main
for i in {1..5}
do
     echo "start on 6 proc, iter № $i"
     mpirun -oversubscribe -n 6 ./main
     echo "done on 6 proc, iter № $i"
done



