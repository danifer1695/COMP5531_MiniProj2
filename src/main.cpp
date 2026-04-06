#include <iostream>     // cout, cerr
#include <fstream>      // ifstream, ofstream
#include <vector>       // vector
#include <string>       // string
#include <algorithm>    // sort
#include <cstring>      // memset, memcpy


#include "../include/Record.h"

#include <filesystem>
namespace fs = std::filesystem;

using namespace std;


// ======================================================
// Overview
// ======================================================


// ======================================================
// Constants
// ======================================================

const int RECORDS_PER_BLOCK = 40;

// ======================================================
// Data Structures
// ======================================================


struct Block
{
    static const int RECORDS_PER_BLOCK = 40;
    Record records[RECORDS_PER_BLOCK];
    int count = 0;

    // for testing purposes
    static const int MAX = 2;
};

struct RunInfo
{
    string filename;
    int runNumber;
};

struct CompactRecord
{
    string data;   // stores "tuple:count"
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
Record parseRecordFromLine(const string& line);

// ---- Phase 1 ----
bool readBlock(ifstream& inputFile, Block& block);
vector<Record> readChunk(ifstream& inputFile, int memoryBlockLimit);
void sortChunk(vector<Record>& chunk);
RunInfo writeRun(const vector<Record>& chunk, int runNumber, const string& prefix);
vector<RunInfo> createInitialRuns(const string& inputFilename, int memoryBlockLimit, const string& prefix);

// ---- Phase 1 ----
bool readNextBlockFromRun(ifstream& runFile, Block& block);
bool getNextRecordFromRun(ifstream& runFile, Block& block, int& blockIndex, Record& record);
RunInfo mergeTwoRuns(const RunInfo& runA, const RunInfo& runB, const string& prefix, int passNumber, int outputRunNumber);
vector<RunInfo> mergePass(const vector<RunInfo>& inputRuns, const string& prefix, int passNumber);
RunInfo mergeAllRuns(const vector<RunInfo>& initialRuns, const string& prefix);

// ---- Bag Union ----

int countCurrentRunCopies(ifstream& runFile, Block& block, int& index, Record& currentRecord, bool& hasRecord);
void writeCompactRecord(ofstream& outFile, const Record& record, int count);
void bagUnion(const RunInfo& sortedR1, const RunInfo& sortedR2, const string& outputFilename);

//helpers
CompactRecord makeCompactRecord(const Record& record, int count);
void flushCompactBlock(ofstream& outFile, CompactBlock& block);
void appendCompactRecord(ofstream& outFile, CompactBlock& block, const Record& record, int count);


// ======================================================
// Main
// ======================================================
int main()
{
    // set up file system output folders
    fs::create_directory("runs");
    fs::create_directory("runs/R1");
    fs::create_directory("runs/R2");
    fs::create_directory("runs/merged");
    fs::create_directory("output");
    


    // Input relation files
    string inputFileR1 = "R1.txt";
    string inputFileR2 = "R2.txt";
    int memoryBlockLimit = 5;

    // Phase 1 + Phase 2 for R1
    cout << "Processing R1..." << endl;
    vector<RunInfo> initialRunsR1 = createInitialRuns(inputFileR1, memoryBlockLimit, "R1");
    RunInfo sortedR1 = mergeAllRuns(initialRunsR1, "R1");
    cout << "R1 fully sorted into file: " << sortedR1.filename << endl;

    // Phase 1 + Phase 2 for R2
    cout << "\nProcessing R2..." << endl;
    vector<RunInfo> initialRunsR2 = createInitialRuns(inputFileR2, memoryBlockLimit, "R2");
    RunInfo sortedR2 = mergeAllRuns(initialRunsR2, "R2");
    cout << "R2 fully sorted into file: " << sortedR2.filename << endl;

    // Final bag union
    cout << "\nPerforming bag union..." << endl;
    string outputFile = "output/bag_union_output.txt";
    bagUnion(sortedR1, sortedR2, outputFile);

    cout << "Bag union complete. Output written to: " << outputFile << endl;

    return 0;
}




// ======================================================
// Function Definitions - phase 1
// ======================================================

// parseRecordFromLine

Record parseRecordFromLine(const string& line)
{
    Record r;

    // Fill with spaces
    memset(r.data, ' ', Record::SIZE);

    // Determine how many bytes we can safely copy
    int bytesToCopy = min((int)line.size(), Record::SIZE);

    // Copy line into r.data (max 100 bytes)
    memcpy(r.data, line.c_str(), bytesToCopy);

    return r;
}


//readBlock
bool readBlock(ifstream& inputFile, Block& block)
{
    // Reset block
    block.count = 0;

    string line;

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

vector<Record> readChunk(ifstream& inputFile, int memoryBlockLimit)
{
    vector<Record> chunk;

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
void sortChunk(vector<Record>& chunk)
{
    sort(chunk.begin(), chunk.end());
}

//writeRun
RunInfo writeRun(const vector<Record>& chunk, int runNumber, const string& prefix)
{
    RunInfo run;

    run.runNumber = runNumber;
    run.filename = "runs/" + prefix + "/" + prefix + "_run_" + to_string(runNumber) + ".txt";

    ofstream outputFile(run.filename);

    if (!outputFile)
    {
        cerr << "Error: could not create " << run.filename << endl;
        return run;
    }

    for (const auto& record : chunk)
    {
        outputFile.write(record.data, Record::SIZE);
        outputFile << '\n';
    }

    cout << "Writing " << run.filename << " with " << chunk.size() << " records\n";

    outputFile.close();
    return run;
}


//createInitialRuns
vector<RunInfo> createInitialRuns(const string& inputFilename, int memoryBlockLimit, const string& prefix)
{
    vector<RunInfo> runs;

    ifstream inputFile(inputFilename);

    if (!inputFile)
    {
        cerr << "Error: could not open " << inputFilename << endl;
        return runs;
    }

    int runNumber = 0;

    while (true)
    {
        vector<Record> chunk = readChunk(inputFile, memoryBlockLimit);

        if (chunk.empty())
        {
            break;
        }

        cout << prefix << " chunk size: " << chunk.size() << endl;

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


bool readNextBlockFromRun(ifstream& runFile, Block& block)
{
    // 1. reset block.count
    block.count = 0;

    // 2. create string line
    string line;

    // 3. loop:
    //    while block not full AND getline succeeds

    while (block.count < Block::RECORDS_PER_BLOCK && getline(runFile, line)){

    
    {   
        // 4. parse line into Record
        Record r = parseRecordFromLine(line);

        // 5. store into block.records
        block.records[block.count] = r;

        // 6. increment count
        block.count++;
    }
    
    }
    return block.count > 0;

}

bool getNextRecordFromRun(ifstream& runFile, Block& block, int& index, Record& record)
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

RunInfo mergeTwoRuns(const RunInfo& runA, const RunInfo& runB, const string& prefix, int passNumber, int outputRunNumber)
{
    RunInfo outputRun;
    outputRun.runNumber = outputRunNumber;
    outputRun.filename = "runs/merged/" + prefix + "_pass_" + to_string(passNumber) + "_run_" + to_string(outputRunNumber) + ".txt";

    ifstream fileA(runA.filename);
    ifstream fileB(runB.filename);

    if (!fileA)
    {
        cerr << "Error: could not open " << runA.filename << endl;
        return outputRun;
    }

    if (!fileB)
    {
        cerr << "Error: could not open " << runB.filename << endl;
        return outputRun;
    }

    ofstream outFile(outputRun.filename);

    if (!outFile)
    {
        cerr << "Error: could not create " << outputRun.filename << endl;
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
            outFile.write(recordA.data, Record::SIZE);
            outFile << '\n';
            hasA = getNextRecordFromRun(fileA, blockA, indexA, recordA);
        }
        else
        {
            outFile.write(recordB.data, Record::SIZE);
            outFile << '\n';
            hasB = getNextRecordFromRun(fileB, blockB, indexB, recordB);
        }
    }

    while (hasA)
    {
        outFile.write(recordA.data, Record::SIZE);
        outFile << '\n';
        hasA = getNextRecordFromRun(fileA, blockA, indexA, recordA);
    }

    while (hasB)
    {
        outFile.write(recordB.data, Record::SIZE);
        outFile << '\n';
        hasB = getNextRecordFromRun(fileB, blockB, indexB, recordB);
    }

    fileA.close();
    fileB.close();
    outFile.close();

    return outputRun;
}

vector<RunInfo> mergePass(const vector<RunInfo>& inputRuns, const string& prefix, int passNumber)
{
    vector<RunInfo> outputRuns;
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

RunInfo mergeAllRuns(const vector<RunInfo>& initialRuns, const string& prefix)
{
    vector<RunInfo> currentRuns = initialRuns;
    int passNumber = 0;

    while (currentRuns.size() > 1)
    {
        cout << prefix << " pass " << passNumber << ": " << currentRuns.size() << " runs\n";
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


int countCurrentRunCopies(ifstream& runFile, Block& block, int& index, Record& currentRecord, bool& hasRecord)
{
    // currentRecord is the first copy
    Record target = currentRecord;
    int count = 1;

    // Advance once to look ahead
    hasRecord = getNextRecordFromRun(runFile, block, index, currentRecord);

    // Keep counting while the next record matches the target
    while (hasRecord && currentRecord == target)
    {
        count++;
        hasRecord = getNextRecordFromRun(runFile, block, index, currentRecord);
    }

    return count;
}

void writeCompactRecord(ofstream& outFile, const Record& record, int count)
{
    // Write the full 100-byte record
    outFile.write(record.data, Record::SIZE);

    // Separator between tuple and count
    outFile << ":";

    // Write the count
    outFile << count;

    // End the line
    outFile << '\n';
}

void bagUnion(const RunInfo& sortedR1, const RunInfo& sortedR2, const string& outputFilename)
{
    ifstream fileR1(sortedR1.filename);
    ifstream fileR2(sortedR2.filename);
    ofstream outFile(outputFilename);

    if (!fileR1)
    {
        cerr << "Error: could not open " << sortedR1.filename << endl;
        return;
    }

    if (!fileR2)
    {
        cerr << "Error: could not open " << sortedR2.filename << endl;
        return;
    }

    if (!outFile)
    {
        cerr << "Error: could not create " << outputFilename << endl;
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
    outFile.close();
}

// helper functions
CompactRecord makeCompactRecord(const Record& record, int count)
{
    CompactRecord cr;
    cr.data = string(record.data, Record::SIZE) + ":" + to_string(count);
    return cr;
}

void flushCompactBlock(ofstream& outFile, CompactBlock& block)
{
    for (int i = 0; i < block.count; i++)
    {
        outFile << block.records[i].data << '\n';
    }

    block.count = 0;
}

void appendCompactRecord(ofstream& outFile, CompactBlock& block, const Record& record, int count)
{
    block.records[block.count] = makeCompactRecord(record, count);
    block.count++;

    if (block.count == CompactBlock::MAX)
    {
        flushCompactBlock(outFile, block);
    }
}


