#ifndef PARSER_H
#define PARSER_H

#include <vector>
#include <string>

void processRecords(std::vector<std::string>& records, int rank);
void aggregateRecords(std::vector<std::string>& data);

#endif 
