// #include <algorithm>
// #include <fstream>
// #include <iostream>
// #include <vector>
// #include <string>
// #include <sstream>
// #include <unordered_map>
// #include <omp.h>
// #include <mpi.h>
// #include <cmath>
// #include <unordered_set>
// #include <map>
 
// namespace MolecularWeightKeys {
//     const std::string CO = "CO";
//     const std::string NO2 = "NO2";
//     const std::string OZONE = "OZONE";
//     const std::string PM10 = "PM10";
//     const std::string PM25 = "PM2.5"; 
//     const std::string SO2 = "SO2";
// }
 
// std::unordered_map<std::string, double> molecularWeights {
//         {MolecularWeightKeys::CO, 28.01},    
//         {MolecularWeightKeys::NO2, 46.01},   
//         {MolecularWeightKeys::OZONE, 48.0},  
//         {MolecularWeightKeys::PM10, 16.01},  
//         {MolecularWeightKeys::PM25, 12.01},  
//         {MolecularWeightKeys::SO2, 64.06}    
// };
 
// std::unordered_map<std::string, int> HeaderNameToIndex = {
//         {"Latitude", 0},
//         {"Longitude", 1},
//         {"UTC Date and time", 2},
//         {"Parameter", 3},
//         {"Concentration", 4},
//         {"Unit", 5},
//         {"Raw Concentration", 6},
//         {"AQI", 7},
//         {"Category", 8},
//         {"Site Name", 9},
//         {"Site Agency", 10},
//         {"AQS ID", 11},
//         {"Full AQS ID", 12}
// };
 
// namespace ColumnNames {
//     const std::string Latitude = "Latitude";
//     const std::string Longitude = "Longitude";
//     const std::string UTCDateTime = "UTC Date and time";
//     const std::string Parameter = "Parameter";
//     const std::string Concentration = "Concentration";
//     const std::string Unit = "Unit";
//     const std::string RawConcentration = "Raw Concentration";
//     const std::string AQI = "AQI";
//     const std::string Category = "Category";
//     const std::string SiteName = "Site Name";
//     const std::string SiteAgency = "Site Agency";
//     const std::string AQSId = "AQS ID";
//     const std::string FullAQSId = "Full AQS ID";
// }

// std::string normalizeUnit(std::string value, std::string unit,std::string parameter){
//     try {
//         std::string input = value;
//         value.erase(std::remove(value.begin(), value.end(), '"'), value.end());
//         unit.erase(std::remove(unit.begin(), unit.end(), '"'), unit.end());
//         parameter.erase(std::remove(parameter.begin(), parameter.end(), '"'), parameter.end());
//         double concentration = std::stod(value);

    
//         if(unit == "UG/M3"){
//             return input;
//         }

//         double molecularWeight = molecularWeights[parameter];
 
//         // Convert PPB or PPM to UG/M3
//         double concentrationUGPerM3;
//         if (unit == "PPB") {
//             concentrationUGPerM3 = (concentration * molecularWeight) / 24.45;
//         } else { // unit == "PPM"
//             concentrationUGPerM3 = (concentration * molecularWeight) / (24.45 * 1000);
//         }
 
//         // Convert concentration to string with appropriate precision
//         std::ostringstream oss;
//         oss << std::fixed << concentrationUGPerM3;
//         return "\""+oss.str()+"\"";
//     } catch(const std::invalid_argument& e){
//         return "INVALID";
//     }
// }
 
 
// void processRecords(std::vector<std::string>& records, int rank) {
//     int numRecords = records.size();
//     std::vector<std::string> updatedRecords;

//     updatedRecords.reserve(numRecords);
 
//     std::ostringstream msg;
//     double processStartTime = MPI_Wtime();  // Start timing
 
//     msg << "Starting time of processRecords: " << processStartTime * 1000 << " milliseconds.";
//     msg.str("");
 
//     msg << "Starting to process records in parallel. Total records: " << numRecords;
//     msg.str("");
 
//     // Define the indices of the columns to be dropped
//     std::vector<int> columnsToDrop;
//     columnsToDrop.push_back(HeaderNameToIndex[ColumnNames::AQSId]);
//     columnsToDrop.push_back(HeaderNameToIndex[ColumnNames::FullAQSId]);
//     columnsToDrop.push_back(HeaderNameToIndex[ColumnNames::Unit]);
 
//     std::unordered_map<int, int> recordsPerMillisecondGlobal;
 
//     #pragma omp parallel
//     {
//         std::unordered_map<int, int> recordsPerMillisecondLocal; // Thread-local metrics collection
 
//         #pragma omp for nowait
//         for (int i = 0; i < numRecords; ++i) {
//             std::stringstream ss(records[i]);
//             std::string token;
//             std::vector<std::string> fields;
            
//             // Split record into fields
//             while (getline(ss, token, ',')) {
//                 fields.push_back(token);
//             }
//             bool shouldSkipRow = false;
//             // Prepare to reconstruct the record without dropped columns
//             std::string reconstructedRecord;
//             for (size_t j = 0; j < fields.size(); ++j) {
//                 // Skip columns marked to be dropped
//                 if (std::find(columnsToDrop.begin(), columnsToDrop.end(), static_cast<int>(j)) != columnsToDrop.end()) {
//                     continue;
//                 }
 
//                 // Append "NULL" for empty fields except for columns to be dropped
//                 std::string fieldValue = fields[j].empty() ? "NULL" : fields[j];
//                 if(fieldValue == "\"-999\"" || fieldValue == "\"-999.0\""){
//                     shouldSkipRow = true;
//                     break;
//                 }
 
//                 if(j == HeaderNameToIndex[ColumnNames::Concentration]){
//                     fieldValue = normalizeUnit(fieldValue,fields[HeaderNameToIndex[ColumnNames::Unit]],fields[HeaderNameToIndex[ColumnNames::Parameter]]);
//                 }
 
//                 // Construct the updated record
//                 if (!reconstructedRecord.empty()) {
//                     reconstructedRecord += ",";
//                 }
//                 reconstructedRecord += fieldValue;
//             }
           
//             // if(!shouldSkipRow){
//                 updatedRecords.push_back(reconstructedRecord);
//             // }
//             // Metrics collection
//             double currentTime = MPI_Wtime(); // Current time in seconds
//             int elapsedMilliseconds = static_cast<int>(currentTime - processStartTime); 
//             recordsPerMillisecondLocal[elapsedMilliseconds]++;
//         }
 
//         // Combine local metrics into a global structure
//         #pragma omp critical
//         {
//             for (auto& pair : recordsPerMillisecondLocal) {
//                 recordsPerMillisecondGlobal[pair.first] += pair.second;
//             }
//         }
//     }
//     std::cout<<"Rank " << rank << " Records size : "<< records.size() << " Updated records size " << updatedRecords.size()<<std::endl;
//     records = updatedRecords; // Update original records with processed ones
//     std::cout<<"Rank " << rank <<" Records size : "<< records.size() << " Updated records size " << updatedRecords.size()<<std::endl;
//     // records.erase(std::remove_if(records.begin(), records.end(), 
//     //                           [](const auto& record){ return record.empty(); }),
//     //           records.end());
//     // Export the cleaned data to a CSV file
//     std::ofstream outputFile("../data/cleaned_data_rank_" + std::to_string(rank) + ".csv");
//     if (outputFile.is_open()) {
//         for (const auto& record : records) {
//             if(!record.empty())
//             outputFile << record << "\n";
//         }
//         outputFile.close();
//         msg.str("");
//         msg << "Cleaned data exported to CSV file for rank: " + std::to_string(rank);
//     } else {
//         msg.str("");
//         msg << "Failed to open CSV file for writing for rank: " + std::to_string(rank);
//     }
 
//     // Output metrics to a file
//     std::ofstream metricsFile("../data/records_per_second_process_" + std::to_string(rank) + ".csv");
//     if (!metricsFile) {
//         std::cerr << "Failed to open metrics file for writing." << std::endl;
//         return;
//     }
 
//     metricsFile << "Seconds,RecordsProcessed\n";
//     for (const auto& pair : recordsPerMillisecondGlobal) {
//         metricsFile << pair.first << "," << pair.second << "\n";
//     }
 
//     metricsFile.close();
 
//     msg.str("");
//     msg << "Finished processing records in parallel. Total records processed: " << numRecords;
 
//     // Log the end of processing
//     double processEndTime = MPI_Wtime();  // End timing
//     double processTime = processEndTime - processStartTime;
 
//     msg.str("");
//     msg<< "Ending time of processRecords: " << processEndTime * 1000 << " millisecondsseconds.";
//     msg.str("");
//     msg << "Process " << rank << " processing time: " << processTime << " seconds.";
// }
#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <omp.h> // Include OpenMP for parallel processing
#include <mpi.h>

void processRecords(std::vector<std::string>& records, int rank) {
    int numRecords = records.size();
    std::vector<std::string> updatedRecords(numRecords);

    std::ostringstream msg;
    double processStartTime = MPI_Wtime();  // Start timing

    msg << "Starting time of processRecords: " << processStartTime * 1000 << " milliseconds.";
    msg.str("");

    msg << "Starting to process records in parallel. Total records: " << numRecords;
    msg.str("");

    // Define the indices of the columns to be dropped
    std::vector<int> columnsToDrop = { 12 };

    std::unordered_map<int, int> recordsPerMillisecondGlobal;

    #pragma omp parallel
    {
        std::unordered_map<int, int> recordsPerMillisecondLocal; // Thread-local metrics collection

        #pragma omp for nowait
        for (int i = 0; i < numRecords; ++i) {
            std::stringstream ss(records[i]);
            std::string token;
            std::vector<std::string> fields;
            
            // Split record into fields
            while (getline(ss, token, ',')) {
                fields.push_back(token);
            }
            
            // Prepare to reconstruct the record without dropped columns
            std::string reconstructedRecord;
            for (size_t j = 0; j < fields.size(); ++j) {
                // Skip columns marked to be dropped
                if (std::find(columnsToDrop.begin(), columnsToDrop.end(), static_cast<int>(j)) != columnsToDrop.end()) {
                    continue;
                }

                // Append "NULL" for empty fields except for columns to be dropped
                std::string fieldValue = fields[j].empty() ? "NULL" : fields[j];

                // Construct the updated record
                if (!reconstructedRecord.empty()) {
                    reconstructedRecord += ",";
                }
                reconstructedRecord += fieldValue;
            }

            updatedRecords[i] = reconstructedRecord;

            // Metrics collection
            double currentTime = MPI_Wtime(); // Current time in seconds
            int elapsedMilliseconds = static_cast<int>(currentTime - processStartTime); 
            recordsPerMillisecondLocal[elapsedMilliseconds]++;
        }

        // Combine local metrics into a global structure
        #pragma omp critical
        {
            for (auto& pair : recordsPerMillisecondLocal) {
                recordsPerMillisecondGlobal[pair.first] += pair.second;
            }
        }
    }

    records.swap(updatedRecords); // Update original records with processed ones

    // Export the cleaned data to a CSV file
    std::ofstream outputFile("../data/cleaned_data_rank_" + std::to_string(rank) + ".csv");
    if (outputFile.is_open()) {
        for (const auto& record : records) {
            outputFile << record << std::endl;
        }
        outputFile.close();
        msg.str("");
        msg << "Cleaned data exported to CSV file for rank: " + std::to_string(rank);
    } else {
        msg.str("");
        msg << "Failed to open CSV file for writing for rank: " + std::to_string(rank);
    }

    // Output metrics to a file
    std::ofstream metricsFile("../data/records_per_second_process_" + std::to_string(rank) + ".csv");
    if (!metricsFile) {
        std::cerr << "Failed to open metrics file for writing." << std::endl;
        return;
    }

    metricsFile << "Seconds,RecordsProcessed\n";
    for (const auto& pair : recordsPerMillisecondGlobal) {
        metricsFile << pair.first << "," << pair.second << "\n";
    }

    metricsFile.close();

    msg.str("");
    msg << "Finished processing records in parallel. Total records processed: " << numRecords;

    // Log the end of processing
    double processEndTime = MPI_Wtime();  // End timing
    double processTime = processEndTime - processStartTime;

    msg.str("");
    msg<< "Ending time of processRecords: " << processEndTime * 1000 << " millisecondsseconds.";
    msg.str("");
    msg << "Process " << rank << " processing time: " << processTime << " seconds.";
}