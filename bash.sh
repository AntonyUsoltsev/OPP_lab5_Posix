# shellcheck disable=SC1073
for i in {1..20}
do
     echo "start on 2 proc, iter № $i"
     mpirun -oversubscribe -n 2 ./main
     echo "done on 2 proc, iter № $i"
done
for i in {1..20}
do
     echo "start on 4 proc, iter №$i"
     mpirun -oversubscribe -n 4 ./main
     echo "done on 4 proc, iter № $i"
done
for i in {1..20}
do
     echo "start on 8 proc, iter № $i"
     mpirun -oversubscribe -n 8 ./main
     echo "done on 8 proc, iter № $i"
done


