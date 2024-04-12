#include <iostream>
#include <fstream>
#include <mpi.h>
#include <string>
#include <vector>
#include <sstream>

// New function to calculate offsets
void calculateOffsets(const std::string& filePath, std::vector<long>& offsets) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open file for offset calculation.\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    offsets.push_back(0);  // The first record starts at offset 0
    char c;
    while (file.get(c)) {
        if (c == '\n') {  // Assuming Unix-style line endings
            // Save the position of the start of the next line
            offsets.push_back(file.tellg());
        }
    }
    file.close();
}

void distributeRecords(const std::string& filePath, int rank, int size, std::vector<std::string>& records) {
    std::vector<long> allOffsets;
    if (rank == 0) {
        calculateOffsets(filePath, allOffsets);
    }

    // Number of records is one less than the number of offsets since the last offset marks the end of the file
    int totalCount = allOffsets.size() - 1;

    // Broadcast totalCount to all processes since it's needed for calculating ranges
    MPI_Bcast(&totalCount, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Determine the range of records for this process
    int recordsPerProcess = totalCount / size;
    int remainingRecords = totalCount % size;

    int startRecord = rank * recordsPerProcess + std::min(rank, remainingRecords);
    int endRecord = startRecord + recordsPerProcess + (rank < remainingRecords ? 1 : 0);

    // Now, distribute the offsets for only the required records to each process
    long startOffset, endOffset;
    if (rank == 0) {
        // Send the specific starting and ending offsets to each process
        for (int i = 0; i < size; i++) {
            int startIdx = i * recordsPerProcess + std::min(i, remainingRecords);
            int endIdx = startIdx + recordsPerProcess + (i < remainingRecords ? 1 : 0) - 1; // -1 because endIdx should be inclusive
            long offsetsToSend[2] = {allOffsets[startIdx], allOffsets[endIdx + 1]}; // +1 to include the newline character of the last record
            if (i == 0) {
                startOffset = offsetsToSend[0];
                endOffset = offsetsToSend[1];
            } else {
                MPI_Send(&offsetsToSend, 2, MPI_LONG, i, 0, MPI_COMM_WORLD);
            }
        }
    } else {
        long receivedOffsets[2];
        MPI_Recv(&receivedOffsets, 2, MPI_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        startOffset = receivedOffsets[0];
        endOffset = receivedOffsets[1];
    }

    std::ostringstream msg;
    msg << "Process " << rank << " reading from offset " << startOffset << " to " << endOffset;

    // Each process reads its assigned portion of the file based on offsets
    std::ifstream file(filePath, std::ios::binary);
    file.seekg(startOffset);
    std::string line;
    while (file.tellg() < endOffset && std::getline(file, line)) {
        records.push_back(line);
    }

    msg.str("");
    msg << "Process " << rank << " finished reading. Total records read: " << records.size();
}
