#pragma once

#include<vector>
#include<stdio.h>

#include "Record.h"

class DiskManager
{
private:

	//Number of block reads and writes performed
	int readCount;
	int writeCount;

public:

	static constexpr int BLOCK_SIZE = 4096;
	static constexpr int RECORDS_PER_BLOCK = 40;
	static constexpr int DATA_PER_BLOCK = RECORDS_PER_BLOCK * Record::SIZE;
	static constexpr int BLOCK_PADDING = BLOCK_SIZE - DATA_PER_BLOCK;		//96 bytes of padding

	//Constructor declarations
	//======================================================================================
	DiskManager();

	//Methods
	//======================================================================================
	//Reading / Writing (I/O)
	int readBlock(FILE* file, std::vector<Record>& records);
	int readBlocks(FILE* file, std::vector<Record>& records, int count);
	void writeBlock(FILE* file, const std::vector<Record>& records, int count);
	void writeAllRecords(FILE* file, const std::vector<Record>& records);

	//Statistics
	int GetReadCount() const	{ return readCount; }
	int GetWriteCount() const	{ return writeCount; }
	int GetTotalIO() const		{ return readCount + writeCount; }

	void resetCounters()		{ readCount = writeCount = 0; }

	static int countBlocks(const std::string& filePath);
	static int countRecords(const std::string& filePath);
};

