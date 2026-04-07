#include <iostream>     // cout, cerr
#include <fstream>      // ifstream, ofstream
#include <vector>       // vector
#include <string>       // string
#include <algorithm>    // sort
#include <cstring>      // memset, memcpy
#include <filesystem>
#include <chrono>
#include <cstdio>

// ======================================================
// Constants
// ======================================================

enum TEST_SIZE
{
    SMALL,
    MEDIUM,
    LARGE
};

constexpr int RECORDS_PER_BLOCK = 40;
constexpr int BYTES_PER_RECORD = 100;
bool cleanTemps = true;
bool logProgress = true;
TEST_SIZE testSize = SMALL;


// ======================================================
// Data Structures
// ======================================================

struct Record
{
    char data[BYTES_PER_RECORD];	//actual content of the tuple, formated as a C-style string (an array of chars)
    Record() {std::memset(data, '0', BYTES_PER_RECORD); }

    //overridden < operator to be able to use on std::sort
    bool operator<(const Record& other) const {return std::memcmp(data, other.data, BYTES_PER_RECORD) < 0;}

    //to c++ string
    std::string toString() const { return std::string(data, BYTES_PER_RECORD); }

    //Check if empty
    bool isEmpty() const {
        for (int i = 0; i < BYTES_PER_RECORD; i++) {
            if (data[i] != '0') return false;
        }
        return true;
    };
};

struct Block
{
    Record records[RECORDS_PER_BLOCK];
    int count = 0;
};

struct RunInfo
{
    std::string filename;
    int runNumber;
};

struct CompactRecord
{
    std::string data;   // stores "tuple:count"
};

struct CompactBlock
{
    static const int MAX = 40;
    CompactRecord records[MAX];
    int count = 0;
};


// ======================================================
// Function Declarations
// ======================================================
// A block holds up to 40 records.
// count tells us how many valid records are currently in it.

// ---- Parsing ----
Record parseRecordFromLine(const std::string& line);

// ---- Phase 1 ----
bool readBlock(std::ifstream& inputFile, Block& block);
std::vector<Record> readChunk(std::ifstream& inputFile, int memoryBlockLimit);
void sortChunk(std::vector<Record>& chunk);
RunInfo writeRun(const std::vector<Record>& chunk, int runNumber, const std::string& prefix);
std::vector<RunInfo> createInitialRuns(const std::string& inputFilename, int memoryBlockLimit, const std::string& prefix);

// ---- Phase 1 ----
bool readNextBlockFromRun(std::ifstream& runFile, Block& block);
bool getNextRecordFromRun(std::ifstream& runFile, Block& block, int& blockIndex, Record& record);
RunInfo mergeTwoRuns(const RunInfo& runA, const RunInfo& runB, const std::string& prefix, int passNumber, int outputRunNumber);
std::vector<RunInfo> mergePass(const std::vector<RunInfo>& inputRuns, const std::string& prefix, int passNumber);
RunInfo mergeAllRuns(const std::vector<RunInfo>& initialRuns, const std::string& prefix);

// ---- Bag Union ----

int countCurrentRunCopies(std::ifstream& runFile, Block& block, int& index, Record& currentRecord, bool& hasRecord);
void writeCompactRecord(std::ofstream& outFile, const Record& record, int& count);
void bagUnion(const RunInfo& sortedR1, const RunInfo& sortedR2, const std::string& outputFilename);

//helpers
CompactRecord makeCompactRecord(const Record& record, int& count);
void flushCompactBlock(std::ofstream& outFile, CompactBlock& block);
void appendCompactRecord(std::ofstream& outFile, CompactBlock& block, const Record& record, int count);


void Test(const std::string& R1Path, const std::string& R2Path, const int& M, const int& totalRecords)
{
    // set up file system output folders
    std::cout << "==============================================================================" << std::endl;
    std::filesystem::create_directory("../runs");
    std::filesystem::create_directory("../runs/R1");
    std::filesystem::create_directory("../runs/R2");
    std::filesystem::create_directory("../runs/merged");
    std::filesystem::create_directory("../output");
    
    //start timer
    auto begin = std::chrono::high_resolution_clock::now();
    
    // Phase 1 + Phase 2 for R1
    if(logProgress) std::cout << "Processing R1..." << std::endl;
    if(logProgress) std::cout << "Chunk size: " << M * RECORDS_PER_BLOCK << std::endl;
    std::vector<RunInfo> initialRunsR1 = createInitialRuns(R1Path, M, "R1");
    RunInfo sortedR1 = mergeAllRuns(initialRunsR1, "R1");
    std::cout << "\nR1 fully sorted into file: " << sortedR1.filename << std::endl;
    
    // Phase 1 + Phase 2 for R2
    if(logProgress) std::cout << "\nProcessing R2..." << std::endl;
    if(logProgress) std::cout << "Chunk size: " << M * RECORDS_PER_BLOCK << std::endl;
    std::vector<RunInfo> initialRunsR2 = createInitialRuns(R2Path, M, "R2");
    RunInfo sortedR2 = mergeAllRuns(initialRunsR2, "R2");
    std::cout << "R2 fully sorted into file: " << sortedR2.filename << std::endl;
    
    // Final bag union
    if(logProgress) std::cout << "\nPerforming bag union..." << std::endl;
    std::string outFile = "R" + std::to_string(totalRecords) + "M" + std::to_string(M);
    std::string outputFile = "../output/" + outFile + ".txt";
    bagUnion(sortedR1, sortedR2, outputFile);
    
    auto end = std::chrono::high_resolution_clock::now();
    float elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0f;
    
    std::cout << "Bag union complete. Output written to: " << outputFile << std::endl;
    std::cout << "\nTesting Complete - Records: " << totalRecords << " - M: " << M << std::endl;
    std::cout << "Total elapsed time: " << elapsed << " seconds" << std::endl;
    std::cout << "==============================================================================" << std::endl;
}

// ======================================================
// Process Arguments
// ======================================================

void ProcessArgs(int argc, char* argv[])
{
    //arg[0] is the program name "./main.exe"
    if(argc > 1)
    {
        std::string cleanup = argv[1];
        if(cleanup == "cleanup") cleanTemps = true;
        else if(cleanup == "no-cleanup") cleanTemps = false;
        else std::cout << "Invalid argument [1]" << std::endl;
    }

    if(argc > 2)
    {
        std::string log = argv[2];
        if(log == "log") logProgress = true;
        else if(log == "no-log") logProgress = false;
        else std::cout << "Invalid argument [2]" << std::endl;
    }

    if(argc > 3)
    {
        std::string test = argv[3];
        if(test == "small") testSize = SMALL;
        else if(test == "medium") testSize = MEDIUM;
        else if(test == "large") testSize = LARGE;
        else std::cout << "Invalid argument [2]" << std::endl;
    }
}
// ======================================================
// Main
// ======================================================
int main(int argc, char* argv[])
{
    ProcessArgs(argc, argv);
    std::string R1_path;
    std::string R2_path;

    //Records: 30000, M: 101
    R1_path = "../data/T1_15000.txt";
    R2_path = "../data/T2_15000.txt";
    Test(R1_path, R2_path, 101, 30000);

    //Records: 30000, M: 5
    Test(R1_path, R2_path, 5, 30000);

    if(testSize == MEDIUM || testSize == LARGE)
    {
        //Records: 240000, M: 101
        R1_path = "../data/T1_120000.txt";
        R2_path = "../data/T2_120000.txt";
        Test(R1_path, R2_path, 101, 240000);
    
        //Records: 240000, M: 5
        Test(R1_path, R2_path, 5, 240000);
    }

    if(testSize == LARGE)
    {
        //Records: 240000, M: 101
        R1_path = "../data/T1_960000.txt";
        R2_path = "../data/T2_960000.txt";
        Test(R1_path, R2_path, 101, 960000);

        //Records: 240000, M: 5
        Test(R1_path, R2_path, 5, 960000);
    }

    return 0;
}

// ======================================================
// Function Definitions - phase 1
// ======================================================

// parseRecordFromLine
Record parseRecordFromLine(const std::string& line)
{
    Record r;

    // Fill with spaces
    memset(r.data, ' ', BYTES_PER_RECORD);

    // Determine how many bytes we can safely copy
    int bytesToCopy = std::min((int)line.size(), BYTES_PER_RECORD);

    // Copy line into r.data (max 100 bytes)
    memcpy(r.data, line.c_str(), bytesToCopy);

    return r;
}

//readBlock
bool readBlock(std::ifstream& inputFile, Block& block)
{
    // Reset block
    block.count = 0;

    std::string line;

    // Read until block full OR file ends
    while (block.count < RECORDS_PER_BLOCK && getline(inputFile, line))
    {
        // Convert line to Record
        Record r = parseRecordFromLine(line);

        // Store in block
        block.records[block.count] = r;

        // Move to next slot
        block.count++;
    }

    // Return true if we read anything
    return block.count > 0;
}

//readChunk
std::vector<Record> readChunk(std::ifstream& inputFile, int memoryBlockLimit)
{
    std::vector<Record> chunk;

    // Loop for up to memoryBlockLimit blocks
    for (int i = 0; i < memoryBlockLimit; i++)
    {
        Block block;

        // Try to read one block
        bool gotData = readBlock(inputFile, block);

        // If no more data, stop
        if (!gotData)
        {
            break;
        }

        // Copy valid records from block into chunk
        for (int j = 0; j < block.count; j++)
        {
            // TODO: push block.records[j] into chunk
            chunk.push_back(block.records[j]);
        }
    }

    return chunk;
}

//sortChunk
void sortChunk(std::vector<Record>& chunk)
{
    std::sort(chunk.begin(), chunk.end());
}

//writeRun
RunInfo writeRun(
    const std::vector<Record>& chunk, 
    int runNumber,  
    const std::string& prefix)
{
    RunInfo run;

    run.runNumber = runNumber;
    run.filename = "../runs/" + prefix + "/" + prefix + "_run_" + std::to_string(runNumber) + ".txt";

    std::ofstream outputFile(run.filename);

    if (!outputFile)
    {
        std::cerr << "Error: could not create " << run.filename << std::endl;
        return run;
    }

    for (const auto& record : chunk)
    {
        outputFile.write(record.data, BYTES_PER_RECORD);
        outputFile << '\n';
    }

    if(logProgress) std::cout << "Writing " << run.filename << " with " << chunk.size() << " records\n";

    outputFile.close();
    return run;
}


//createInitialRuns
std::vector<RunInfo> createInitialRuns(
    const std::string& inputFilename, 
    int memoryBlockLimit, 
    const std::string& prefix)
{
    std::vector<RunInfo> runs;

    std::ifstream inputFile(inputFilename);

    if (!inputFile)
    {
        std::cerr << "Error: could not open " << inputFilename << std::endl;
        return runs;
    }

    int runNumber = 0;

    while (true)
    {
        std::vector<Record> chunk = readChunk(inputFile, memoryBlockLimit);

        if (chunk.empty())
        {
            break;
        }

        //std::cout << prefix << " chunk size: " << chunk.size() << std::endl;

        sortChunk(chunk);

        RunInfo run = writeRun(chunk, runNumber, prefix);
        runs.push_back(run);

        runNumber++;
    }

    inputFile.close();
    return runs;
}

// ======================================================
// Function Definitions - phase 2
// ======================================================

bool readNextBlockFromRun(std::ifstream& runFile, Block& block)
{
    // 1. reset block.count
    block.count = 0;

    // 2. create string line
    std::string line;

    // 3. loop:
    // while block not full AND getline succeeds

    while (block.count < RECORDS_PER_BLOCK && getline(runFile, line))
    {   
        // 4. parse line into Record
        Record r = parseRecordFromLine(line);

        // 5. store into block.records
        block.records[block.count] = r;

        // 6. increment count
        block.count++;
    }
    return block.count > 0;

}

bool getNextRecordFromRun(std::ifstream& runFile, Block& block, int& index, Record& record)
{
    // If current block is exhausted, load next block
    if (index >= block.count)
    {
        bool hasMore = readNextBlockFromRun(runFile, block);
        if (!hasMore)
        {
            return false; // no more data in this run
        }

        index = 0; // reset index for new block
    }

    // Get current record
    record = block.records[index];

    // Move forward
    index++;

    return true;
}

RunInfo mergeTwoRuns(const RunInfo& runA, const RunInfo& runB, const std::string& prefix, int passNumber, int outputRunNumber)
{
    RunInfo outputRun;
    outputRun.runNumber = outputRunNumber;
    outputRun.filename = "../runs/merged/" + prefix + 
        "_pass_" + std::to_string(passNumber) + 
        "_run_" + std::to_string(outputRunNumber) + 
        ".txt";

    std::ifstream fileA(runA.filename);
    std::ifstream fileB(runB.filename);

    if (!fileA)
    {
        std::cerr << "Error: could not open " << runA.filename << std::endl;
        return outputRun;
    }

    if (!fileB)
    {
        std::cerr << "Error: could not open " << runB.filename << std::endl;
        return outputRun;
    }

    std::ofstream outFile(outputRun.filename);

    if (!outFile)
    {
        std::cerr << "Error: could not create " << outputRun.filename << std::endl;
        return outputRun;
    }

    Block blockA;
    Block blockB;

    int indexA = 0;
    int indexB = 0;

    Record recordA;
    Record recordB;

    bool hasA = getNextRecordFromRun(fileA, blockA, indexA, recordA);
    bool hasB = getNextRecordFromRun(fileB, blockB, indexB, recordB);

    while (hasA && hasB)
    {
        if (recordA < recordB)
        {
            outFile.write(recordA.data, BYTES_PER_RECORD);
            outFile << '\n';
            hasA = getNextRecordFromRun(fileA, blockA, indexA, recordA);
        }
        else
        {
            outFile.write(recordB.data, BYTES_PER_RECORD);
            outFile << '\n';
            hasB = getNextRecordFromRun(fileB, blockB, indexB, recordB);
        }
    }

    while (hasA)
    {
        outFile.write(recordA.data, BYTES_PER_RECORD);
        outFile << '\n';
        hasA = getNextRecordFromRun(fileA, blockA, indexA, recordA);
    }

    while (hasB)
    {
        outFile.write(recordB.data, BYTES_PER_RECORD);
        outFile << '\n';
        hasB = getNextRecordFromRun(fileB, blockB, indexB, recordB);
    }

    //close files and clean up
    fileA.close();
    fileB.close();

    if(cleanTemps){
        if(std::remove(runA.filename.c_str()) == 0)
        {
        // std::cout << "Removed temp file " << runA.filename << std::endl;
        }
        if(std::remove(runB.filename.c_str()) == 0)
        { 
        //std::cout << "Removed temp file " << runB.filename << std::endl;
        }
    }

    outFile.close();

    return outputRun;
}

std::vector<RunInfo> mergePass(const std::vector<RunInfo>& inputRuns, const std::string& prefix, int passNumber)
{
    std::vector<RunInfo> outputRuns;
    int outputRunNumber = 0;

    for (int i = 0; i < inputRuns.size(); i += 2)
    {
        if (i + 1 < inputRuns.size())
        {
            RunInfo mergedRun = mergeTwoRuns(inputRuns[i], inputRuns[i + 1], prefix, passNumber, outputRunNumber);
            outputRuns.push_back(mergedRun);
            outputRunNumber++;
        }
        else
        {
            outputRuns.push_back(inputRuns[i]);
        }
    }

    return outputRuns;
}

RunInfo mergeAllRuns(const std::vector<RunInfo>& initialRuns, const std::string& prefix)
{
    std::vector<RunInfo> currentRuns = initialRuns;
    int passNumber = 0;

    while (currentRuns.size() > 1)
    {
        if(logProgress) std::cout << prefix << " pass " << passNumber << ": " << currentRuns.size() << " runs\n";
        currentRuns = mergePass(currentRuns, prefix, passNumber);
        passNumber++;
    }

    if (currentRuns.empty())
    {
        RunInfo emptyRun;
        emptyRun.runNumber = -1;
        emptyRun.filename = "";
        return emptyRun;
    }

    return currentRuns[0];
}

// ======================================================
// Function Definitions - Bag Union
// ======================================================
int countCurrentRunCopies(std::ifstream& runFile, Block& block, int& index, Record& currentRecord, bool& hasRecord)
{
    // currentRecord is the first copy
    Record target = currentRecord;
    int count = 1;

    // Advance once to look ahead
    hasRecord = getNextRecordFromRun(runFile, block, index, currentRecord);

    // Keep counting while the next record matches the target
    while (hasRecord && std::memcmp(currentRecord.data, target.data, BYTES_PER_RECORD) == 0)
    {
        count++;
        hasRecord = getNextRecordFromRun(runFile, block, index, currentRecord);
    }

    return count;
}

void writeCompactRecord(std::ofstream& outFile, const Record& record, int& count)
{
    // Write the full 100-byte record
    outFile.write(record.data, BYTES_PER_RECORD - 1);

    // Separator between tuple and count
    outFile << ":";

    // Write the count
    outFile << count;

    // End the line
    outFile << '\n';
}

void bagUnion(const RunInfo& sortedR1, const RunInfo& sortedR2, const std::string& outputFilename)
{
    std::ifstream fileR1(sortedR1.filename);
    std::ifstream fileR2(sortedR2.filename);
    std::ofstream outFile(outputFilename);

    if (!fileR1)
    {
        std::cerr << "Error: could not open " << sortedR1.filename << std::endl;
        return;
    }

    if (!fileR2)
    {
        std::cerr << "Error: could not open " << sortedR2.filename << std::endl;
        return;
    }

    if (!outFile)
    {
        std::cerr << "Error: could not create " << outputFilename << std::endl;
        return;
    }

    Block blockR1;
    Block blockR2;

    CompactBlock outputBlock;

    int indexR1 = 0;
    int indexR2 = 0;

    Record recordR1;
    Record recordR2;

    bool hasR1 = getNextRecordFromRun(fileR1, blockR1, indexR1, recordR1);
    bool hasR2 = getNextRecordFromRun(fileR2, blockR2, indexR2, recordR2);

    while (hasR1 && hasR2)
    {
        if (recordR1 < recordR2)
        {
            Record target = recordR1;
            int countR1 = countCurrentRunCopies(fileR1, blockR1, indexR1, recordR1, hasR1);

            appendCompactRecord(outFile, outputBlock, target, countR1);
        }
        else if (recordR2 < recordR1)
        {
            Record target = recordR2;
            int countR2 = countCurrentRunCopies(fileR2, blockR2, indexR2, recordR2, hasR2);

            appendCompactRecord(outFile, outputBlock, target, countR2);
        }
        else
        {
            Record target = recordR1;

            int countR1 = countCurrentRunCopies(fileR1, blockR1, indexR1, recordR1, hasR1);
            int countR2 = countCurrentRunCopies(fileR2, blockR2, indexR2, recordR2, hasR2);

            appendCompactRecord(outFile, outputBlock, target, countR1 + countR2);
        }
    }

    while (hasR1)
    {
        Record target = recordR1;
        int countR1 = countCurrentRunCopies(fileR1, blockR1, indexR1, recordR1, hasR1);

        appendCompactRecord(outFile, outputBlock, target, countR1);
    }

    while (hasR2)
    {
        Record target = recordR2;
        int countR2 = countCurrentRunCopies(fileR2, blockR2, indexR2, recordR2, hasR2);

        appendCompactRecord(outFile, outputBlock, target, countR2);
    }

    // flush final partial block
    if (outputBlock.count > 0)
    {
        flushCompactBlock(outFile, outputBlock);
    }

    fileR1.close();
    fileR2.close();
    
    //clean up after we're done with temp files
    if(cleanTemps) std::remove(sortedR1.filename.c_str());
    if(cleanTemps) std::remove(sortedR2.filename.c_str());

    outFile.close();
}

// helper functions
CompactRecord makeCompactRecord(const Record& record, int& count)
{
    CompactRecord cr;
    cr.data = std::string(record.data, BYTES_PER_RECORD) + ":" + std::to_string(count);
    return cr;
}

void flushCompactBlock(std::ofstream& outFile, CompactBlock& block)
{
    for (int i = 0; i < block.count; i++)
    {
        outFile << block.records[i].data << '\n';
    }

    block.count = 0;
}

void appendCompactRecord(std::ofstream& outFile, CompactBlock& block, const Record& record, int count)
{
    block.records[block.count] = makeCompactRecord(record, count);
    block.count++;

    if (block.count == CompactBlock::MAX)
    {
        flushCompactBlock(outFile, block);
    }
}


