# CMPE-275-HPC

Command to run the program: g++-13 -o my_program main.cpp reader.cpp loader.cpp logger.cpp -std=c++17 -fopenmp -I/opt/homebrew/Cellar/open-mpi/5.0.2_1/include -L/opt/homebrew/Cellar/open-mpi/5.0.2_1/lib -lmpi

mpiexec -n 3 ./my_program
