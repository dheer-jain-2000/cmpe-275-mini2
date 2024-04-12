#include <mpi.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include "reader.h"
#include "parser.h"
#include <iostream>
#include <filesystem>
using namespace std;
 
std::vector<std::string> get_csv_files(const std::string& directory_path) {
    std::vector<std::string> file_paths;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(directory_path)) {
        if (entry.path().extension() == ".csv") {
            file_paths.push_back(entry.path());
        }
    }
    return file_paths;
}
 
int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    double startTime = MPI_Wtime();
 
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
 
 
    std::vector<std::string> records;
    std::string data_directory = "/Users/admin/Desktop/CMPE-275-HPC/input_data"; 
    std::vector<std::string> csv_files = get_csv_files(data_directory);
 
    for (const std::string& file_path : csv_files) {
       cout<<"Filepath getting distributed "<<file_path<<endl;
       distributeRecords(file_path, rank, size, records);
    }
    cout<<"*************  Records length is "<<records.size()<<endl;
    cout<<" ** single record looks like ***"<<records[0]<<endl;
    processRecords(records, rank);
 
    // Ensures all processes have finished processing
    MPI_Barrier(MPI_COMM_WORLD); 
 
    if (rank == 0) {
        std::ofstream combinedFile("../final_data/combined_cleaned_data.csv");
        for (int i = 0; i < size; i++) {
            std::string filename = "../data/cleaned_data_rank_" + std::to_string(i) + ".csv";
            // std::string filename = "cleaned_data_process_" + std::to_string(i) + ".csv";
            std::ifstream inputFile(filename);
            if (inputFile.is_open()) {
                combinedFile << inputFile.rdbuf();
                inputFile.close();
            }
        }
        combinedFile.close();
    }
 
    double endTime = MPI_Wtime();  // End timing
    double executionTime = endTime - startTime;
    if (rank == 0) {
        std::cout << "Total Execution Time: " << executionTime << " seconds." << std::endl;
    }
 
    MPI_Finalize();
 
    return 0;
}