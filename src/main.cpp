#include<iostream>
#include<vector>
#include<cstring>
#include<cstdio>
#include<exception>
#include<algorithm>
#include<string>

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
std::vector<std::string>	Phase1(std::string inFile1, std::string outPath);
std::string					CreateRunPath(const std::string& inPath, const std::string& directory, size_t index);
void						FlushOutBlock(Block& outBlock, FILE* outFile);
size_t						MergeRuns(std::vector<LongRun>& runs, Block& outBlock, FILE* outFile);

//Helpers
//-------

void	PrintBlock(const Block& block);
void	OutputBlock(const Block& block, FILE* outFile);
size_t	ReadBlock(Block& block, FILE* inFile);
size_t	CountRecords(const std::string& path);

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

	bool Open(const std::string& path)
	{
		file = std::fopen(path.c_str(), "rb");
		if (!file) return false;
		return Refill();
	}

	//Load next block from memory into buffer
	bool Refill()
	{
		buffer.data.clear();
		size_t count = ReadBlock(buffer, file);
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
	void PopInto(Block& outBlock)
	{
		outBlock.data.push_back(buffer.data.front());
		buffer.data.erase(buffer.data.begin());

		//Once the buffer is drained, reload the next batch from disk
		if (buffer.data.empty())
		{
			Refill();
		}
	}

	bool IsExhausted() const { return exhausted && buffer.data.empty(); }
};

//=====================================================================================================
//TPMMS - Phase 1
//=====================================================================================================
std::vector<std::string> Phase1(std::string inFile1, std::string outPath)
{
	FILE* file1 = std::fopen(inFile1.c_str(), "rb");
	if (!file1) throw std::invalid_argument("File " + inFile1 + " could not be opened.");

	//we create a vector that will the paths for all sorted runs
	std::vector<std::string> runs;

	//we extract block per block, exit once the last one has been reached
	int count = 0;
	while (true)
	{
		Block block = Block();

		//Fill the block and get the number of records it contains
		ReadBlock(block, file1);

		//Use std::sort to sort the block in place
		std::sort(block.data.begin(), block.data.end());

		//If we've reached the end, we output the block
		if (block.data.size() > 0)
		{
			std::string path = CreateRunPath(inFile1, outPath, count);
			FILE* out = std::fopen(path.c_str(), "wb");
			if (!out) throw std::invalid_argument("Could not create file at " + path);

			OutputBlock(block, out);
			std::fclose(out);

			runs.push_back(path);
			count++;
		}

		//if the number of records
		if (block.data.size() < RECORDS_PER_BLOCK) break;
	}
	std::cout<< count << " sorted runs created during Phase 1." << std::endl;

	//Close input file at the end.
	std::fclose(file1);

	return runs;
}

//======================================================
size_t ReadBlock(Block& block, FILE* inFile)
{
	size_t count = 0;

	//Grab 40 Records
	for (int i = 0; i < RECORDS_PER_BLOCK; i++)
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
//TPMMS
//=====================================================================================================

std::string TPMMS(const int M, const std::string& inPath, const std::string& tmpPath)
{
	//PHASE 1
	std::vector<std::string> runPaths = Phase1(inPath, tmpPath);

	//Phase 2
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
				runs[j].Open(runPaths[runsOpened + j]);

			//Create output file for this merged group
			std::string outPath = tmpPath + "pass" + std::to_string(passNum) +
				"_run" + std::to_string(newRuns.size()) + ".txt";
			FILE* outFile = std::fopen(outPath.c_str(), "wb");
			if (!outFile) throw std::invalid_argument("Could not open file " + outPath);
			Block outBlock;

			MergeRuns(runs, outBlock, outFile);
			std::fclose(outFile);

			//Cleanup input runs
			for (size_t j = 0; j < count; j++)
				std::remove(runPaths[runsOpened + j].c_str());

			//Add output files to "newRuns" vector
			newRuns.push_back(outPath);
			runsOpened += count;
		}

		runPaths = newRuns;
		passNum++;
	}

	return runPaths[0];
}

//======================================================
size_t MergeRuns(std::vector<LongRun>& runs, Block& outBlock, FILE* outFile)
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
		runs[smallest].PopInto(outBlock);

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
std::string CreateRunPath(const std::string& inPath, const std::string& directory, size_t index)
{
	//extract file name out of full path
	size_t pos = inPath.find_last_of("/\\");
	std::string fileName = inPath.substr(pos + 1, 2);

	return directory + fileName + "_" + std::to_string(index) + ".txt";
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
//Main
//=====================================================================================================
int main()
{
	std::string R1 = "../data/T1_records.txt";
	std::string R2 = "../data/T2_records.txt";
	std::string OUT_RUNS = "../tmp/";
	std::string OUT = "../output/";
	const int M = 101;

	//Count records in R1
	std::cout << "Records at the beginning: " << CountRecords(R1) << std::endl;

	std::string sortedR1 = TPMMS(M, R1, OUT_RUNS);

	std::cout << "Records after sorting: " << CountRecords(sortedR1) << std::endl;

	return 0;
}