#ifndef READER_H
#define READER_H

#include <string>
#include <vector>

void calculateOffsets(const std::string& filePath, std::vector<long>& offsets);
void distributeRecords(const std::string& filePath, int rank, int size, std::vector<std::string>& records);

#endif // READER_H
