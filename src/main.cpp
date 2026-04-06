#include<iostream>
#include<vector>
#include<cstring>
#include<cstdio>
#include<exception>
#include<algorithm>
#include<string>
#include<chrono>

constexpr int BYTES_PER_RECORD = 100;
constexpr int RECORDS_PER_BLOCK = 40;

//Forward declarations
//====================

struct Record;
struct Block;
struct LongRun;

//PHASE 1
//-------

std::string					TPMMS(const int M, const std::string& inPath, const std::string& tmpPath);
std::vector<std::string>	Phase1(std::string inFile1, std::string outPath, const int& M);
std::string					Phase2(std::vector<std::string> runPaths, const int& M, const std::string& tmpPath, const std::string& inPath);
void						FlushOutBlock(Block& outBlock, FILE* outFile);
size_t						MergeRuns(std::vector<LongRun>& runs, Block& outBlock, FILE* outFile, const int& M);

//Helpers
//-------

void		PrintBlock(const Block& block);
void		OutputBlock(const Block& block, FILE* outFile);
size_t		ReadBlock(Block& block, FILE* inFile, int recordNum);
size_t		CountRecords(const std::string& path);
std::string	ExtractNameFromPath(const std::string& path);
std::string	CreateRunPath(const std::string& inPath, const std::string& directory, size_t index);

//Structs
//=======

struct Record
{
	char* data = new char[100];

	//overriden < operator so we can sort Records
	bool operator<(const Record& other) const { return std::memcmp(data, other.data, 100) < 0; }
};

struct Block
{
	std::vector<Record> data = std::vector<Record>();
};

struct LongRun
{
	Block buffer = Block();	//buffer block
	FILE* file = nullptr;
	bool exhausted = false;

	//CLose file on destroy if its still open
	~LongRun() { if(file != nullptr) std::fclose(file); }

	bool Open(const std::string& path, const int& M)
	{
		file = std::fopen(path.c_str(), "rb");
		if (!file) return false;
		return Refill(M);
	}

	//Load next block from memory into buffer
	bool Refill(const int& M)
	{
		buffer.data.clear();
		size_t count = ReadBlock(buffer, file, RECORDS_PER_BLOCK * M);
		//if file is exhausted, close it
		if (count == 0)
		{
			exhausted = true;
			std::fclose(file);
			return false;
		}
		return true;
	}

	//Get first record
	const Record& GetFirst() const { return buffer.data.front(); }

	//Pop first record into outBlock, refill if buffer is empty
	void PopInto(Block& outBlock, const int& M)
	{
		outBlock.data.push_back(buffer.data.front());
		buffer.data.erase(buffer.data.begin());

		//Once the buffer is drained, reload the next batch from disk
		if (buffer.data.empty())
		{
			Refill(M);
		}
	}

	bool IsExhausted() const { return exhausted && buffer.data.empty(); }
};

//=====================================================================================================
//TPMMS - Phase 1
//=====================================================================================================
std::vector<std::string> Phase1(std::string inFile1, std::string outPath, const int& M)
{
	FILE* file1 = std::fopen(inFile1.c_str(), "rb");
	if (!file1) throw std::invalid_argument("Phase1: File " + inFile1 + " could not be opened.");

	//we create a vector that will the paths for all sorted runs
	std::vector<std::string> runs;

	//we extract block per block, exit once the last one has been reached
	int count = 0;
	while (true)
	{
		LongRun block = LongRun();

		//Fill the block and get the number of records it contains
		ReadBlock(block.buffer, file1, RECORDS_PER_BLOCK * M);

		//Use std::sort to sort the block in place
		std::sort(block.buffer.data.begin(), block.buffer.data.end());

		//If we've reached the end, we output the block
		if (block.buffer.data.size() > 0)
		{
			std::string path = CreateRunPath(inFile1, outPath, count);
			//std::cout << "Run file created: " << path << std::endl;
			FILE* out = std::fopen(path.c_str(), "wb");
			if (!out) throw std::invalid_argument("Phase1: Could not create file at " + path);

			OutputBlock(block.buffer, out);
			std::fclose(out);

			runs.push_back(path);
			count++;
		}

		//if the number of records
		if (block.buffer.data.size() < RECORDS_PER_BLOCK * M) break;
	}
	std::cout<< count << " sorted runs created during Phase 1." << std::endl;

	//Close input file at the end.
	std::fclose(file1);

	return runs;
}

//======================================================


//======================================================
size_t ReadBlock(Block& block, FILE* inFile, int recordNum = 40)
{
	size_t count = 0;

	//Grab 40 Records
	for (int i = 0; i < recordNum; i++)
	{
		//Create a record, a buffer and fill it with spaces
		Record record = Record();
		char buffer[102];
		std::memset(buffer, ' ', BYTES_PER_RECORD);

		//We read into the buffer first. 'fget' reads BYTES_PER_RECORD chars until a terminator character is reached
		char* ptr = std::fgets(buffer, BYTES_PER_RECORD + 2, inFile);

		//If gfets returns null it means we have reached the end of file
		if (!ptr) break;

		//now we copy only the first 100 chars into out records and push it into the block
		count++;
		std::memcpy(record.data, buffer, BYTES_PER_RECORD);
		block.data.push_back(record);
	}

	return count;
}

//======================================================
void OutputBlock(const Block& block, FILE* outPath)
{
	for (Record record : block.data)
	{
		//we create a buffer 1 byte bigger than a Record to make space for 'new line' char
		char buffer[BYTES_PER_RECORD + 1];
		std::memcpy(buffer, record.data, BYTES_PER_RECORD);
		buffer[BYTES_PER_RECORD] = '\n';

		std::fwrite(buffer, 1, 101, outPath);
	}
}

//=====================================================================================================
//TPMMS - Phase 2
//=====================================================================================================

std::string Phase2(std::vector<std::string> runPaths, const int& M, const std::string& tmpPath, const std::string& inPath)
{
	if (runPaths.empty()) throw std::invalid_argument("Phase2: Run path vector cannot be empty.");

	int passNum = 0;
	while (runPaths.size() > 1)
	{
		std::vector<std::string> newRuns;	//New runs generated in this pass
		size_t runsOpened = 0;				//counter of runs that have been processed so far

		while (runsOpened < runPaths.size())
		{
			//Open up to M-1 runs
			size_t count = std::min((size_t)(M - 1), runPaths.size() - runsOpened);
			std::vector<LongRun> runs(count);
			for (size_t j = 0; j < count; j++)
				runs[j].Open(runPaths[runsOpened + j], M);

			//Create output file for this merged group
			std::string outPath =
				tmpPath + ExtractNameFromPath(inPath) +
				"_pass" + std::to_string(passNum) +
				"_run" + std::to_string(newRuns.size()) +
				".txt";

			//std::cout << "Sorted file created: " << outPath << std::endl;
			FILE* outFile = std::fopen(outPath.c_str(), "wb");
			if (!outFile) throw std::invalid_argument("Phase2: Could not open file " + outPath);
			Block outBlock;

			MergeRuns(runs, outBlock, outFile, M);
			std::fclose(outFile);

			//Cleanup input runs
			for (size_t j = 0; j < count; j++)
				std::remove(runPaths[runsOpened + j].c_str());

			//Add output files to "newRuns" vector
			newRuns.push_back(outPath);
			runsOpened += count;
		}

		std::cout << newRuns.size() << " sorted runs created during Phase 2." << std::endl;

		runPaths = newRuns;
		passNum++;
	}

	return runPaths[0];
}

//=====================================================================================================
//TPMMS
//=====================================================================================================

std::string TPMMS(const int M, const std::string& inPath, const std::string& tmpPath)
{
	//PHASE 1
	std::vector<std::string> runPaths = Phase1(inPath, tmpPath, M);

	//PHASE 2
	return Phase2(runPaths, M, tmpPath, inPath);
}

//======================================================
size_t MergeRuns(std::vector<LongRun>& runs, Block& outBlock, FILE* outFile, const int& M)
{
	//counter keeping track of how many inBuffer we've processed
	size_t blocksProcessed = 0;

	//We extract all records in sorted order, and fill output Block
	while (!runs.empty())
	{
		//if outBuffer is full, flush it
		if (outBlock.data.size() >= RECORDS_PER_BLOCK) { 
			FlushOutBlock(outBlock, outFile);
			blocksProcessed++;
		}

		//Find the run with the smallest first record
		size_t smallest = 0;
		for (size_t i = 1; i < runs.size(); i++)
		{
			if (runs[i].GetFirst() < runs[smallest].GetFirst())
				smallest = i;
		}

		//Pop smallest record into out block
		runs[smallest].PopInto(outBlock, M);

		//if run is fully exhausted, remove it
		if (runs[smallest].IsExhausted()) runs.erase(runs.begin() + smallest);
	}

	//Flush any remaining records
	if (!outBlock.data.empty())
	{
		FlushOutBlock(outBlock, outFile);
		blocksProcessed++;
	}

	return blocksProcessed;
}

//======================================================
void FlushOutBlock(Block& outBlock, FILE* outFile)
{
	OutputBlock(outBlock, outFile);
	outBlock = Block();
}

//=====================================================================================================
//Helpers
//=====================================================================================================
void PrintBlock(const Block& block)
{
	for (Record record : block.data)
	{
		std::cout << record.data << std::endl;
	}
}

//======================================================
std::string ExtractNameFromPath(const std::string& path)
{
	//extract file name out of full path
	size_t pos = path.find_last_of("/\\");
	return path.substr(pos + 1, 2);
}

std::string CreateRunPath(const std::string& inPath, const std::string& directory, size_t index)
{
	return directory + ExtractNameFromPath(inPath) + "_" + std::to_string(index) + ".txt";
}

//======================================================
size_t CountRecords(const std::string& path)
{
	FILE* file = std::fopen(path.c_str(), "rb");
	if (!file) return 0;

	size_t count = 0;
	char buffer[102];
	while (std::fgets(buffer, 102, file))
		count++;

	std::fclose(file);
	return count;
}

//=====================================================================================================
//BagJoin
//=====================================================================================================
void BagJoin(std::string& inPath, const std::string& outPath)
{
	//open input file
	FILE* inFile = std::fopen(inPath.c_str(), "rb");
	if (!inFile) throw std::invalid_argument("BagJoin: Could not open file " + inPath);

	//open output file
	FILE* outFile = std::fopen(outPath.c_str(), "wb");
	if (!outFile) throw std::invalid_argument("BagJoin: Could not open file " + outPath);

	//Get first record
	char* last = new char[BYTES_PER_RECORD + 2];
	std::fgets(last, BYTES_PER_RECORD + 2, inFile);

	//Counter for repeated records
	size_t count = 1;

	//loop while the end of file has not been reached
	while (!std::feof(inFile))
	{
		//get next record
		char* current = new char[BYTES_PER_RECORD + 2];
		std::fgets(current, BYTES_PER_RECORD + 2, inFile);

		//compare to see if they are the same. Returns 0 if they are the same
		int equal = (int)std::memcmp(current, last, 100);
		
		//CASE: last and current are equal
		//--------------------------------
		if (equal == 0)
		{
			//We increase the counter
			count++;

			//Set last = current
			std::memcpy(last, current, BYTES_PER_RECORD + 2);
		}
		//CASE: last and current are NOT equal
		//------------------------------------
		else
		{
			// Build the count suffix
			std::string suffix = " :" + std::to_string(count);

			// Write the 100-byte record first
			std::fwrite(last, 1, BYTES_PER_RECORD - 1, outFile);

			// Write the suffix
			std::fwrite(suffix.c_str(), 1, suffix.size(), outFile);

			// Write newline
			std::fwrite("\n", 1, 1, outFile);

			//Reset the counter
			count = 1;

			//Set last = current
			std::memcpy(last, current, BYTES_PER_RECORD + 2);
		}
	}

	std::fclose(inFile);
	std::fclose(outFile);
}

//=====================================================================================================
//Main
//=====================================================================================================
int main()
{
	std::string R1 = "../data/T1_records.txt";
	std::string R2 = "../data/T2_records.txt";
	std::string OUT_RUNS = "../tmp/";
	std::string OUT = "../output/";

	//=======================================================
	//Test: 30000 records, M = 101
	//=======================================================
	{
		std::cout << "==============================================" << std::endl;
		int M = 101;

		std::vector<std::string> sortedRuns;

		auto begin = std::chrono::high_resolution_clock::now();

		std::string sortedR1 = TPMMS(M, R1, OUT_RUNS);
		std::string sortedR2 = TPMMS(M, R2, OUT_RUNS);

		sortedRuns.push_back(sortedR1);
		sortedRuns.push_back(sortedR2);

		std::string finalSorted = Phase2(sortedRuns, M, OUT, "30K_101M");

		auto end = std::chrono::high_resolution_clock::now();
		float elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0f;

		std::cout << "\nRecords processed: " << CountRecords(finalSorted) << std::endl;
		std::cout << "M: " << M << std::endl;
		std::cout << "Time elapsed: " << elapsed << "seconds" << std::endl;

		BagJoin(finalSorted, OUT + "30K_101M.txt");
	}

	//=======================================================
	//Test: 30000 records, M = 5
	//=======================================================
	{
		std::cout << "\n==============================================" << std::endl;
		int M = 5;

		std::vector<std::string> sortedRuns;

		auto begin = std::chrono::high_resolution_clock::now();

		std::string sortedR1 = TPMMS(M, R1, OUT_RUNS);
		std::string sortedR2 = TPMMS(M, R2, OUT_RUNS);

		sortedRuns.push_back(sortedR1);
		sortedRuns.push_back(sortedR2);

		std::string finalSorted = Phase2(sortedRuns, M, OUT, "30K_5M");

		auto end = std::chrono::high_resolution_clock::now();
		float elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0f;

		std::cout << "\nRecords processed: " << CountRecords(finalSorted) << std::endl;
		std::cout << "M: " << M << std::endl;
		std::cout << "Time elapsed: " << elapsed << "seconds" << std::endl;

		BagJoin(finalSorted, OUT + "30K_5M.txt");
	}
	return 0;
}