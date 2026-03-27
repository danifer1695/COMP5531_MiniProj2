#include "../include/DiskManager.h"

//Constructor
//======================================================================================
DiskManager::DiskManager() : readCount(0), writeCount(0)
{
	//readCount and writeCount initialized to 0
}

//readBlock()
//======================================================================================
//This method reads one block (4096 bytes) from disk onto a buffer, separates the data 
// into records, than stores those into given vector of Records "records" (in the function parameters)
//Returns the number of records read from the file
int DiskManager::readBlock(FILE* file, std::vector<Record>& records)
{
	char buffer[BLOCK_SIZE];
	std::memset(buffer, 0, BLOCK_SIZE);	//set entire buffer to 0

	//std::fread: reads given amount of data from a file stream (a file)
	//	buffer		:pointer to the array (memory block) where the data read will be stored
	//	1			:size of each element being read in bytes (how much it reads at a time)
	//	BLOCK_SIZE	:number of elements to be read
	//	file		:pointer to the file stream (to the FILE object)
	//returns: number of elements read.
	size_t bytesRead = std::fread(buffer, 1, BLOCK_SIZE, file);
	if (bytesRead == 0) return 0; //no data read

	//increase the number-of-blocks-read counter
	readCount++;

	//Get the number of records successfully read out of the number of bytes we got from the file
	int dataBytes = std::min((int)bytesRead, DATA_PER_BLOCK);
	int numRecords = dataBytes / Record::SIZE;

	for (int i = 0; i < numRecords; i++)
	{
		Record r;
		std::memcpy(r.data, buffer + (i * Record::SIZE), Record::SIZE);

		//Push current record into "records" vector. Dont include records that are empty
		if (!r.isEmpty()) { records.push_back(r); }
	}

	return numRecords;
}

//readBlock()
//======================================================================================
//This method reads several blocks (4096 bytes) from disk using 'readBlock()'
//Returns the number of records extracted
int DiskManager::readBlocks(FILE* file, std::vector<Record>& records, int count)
{
	int totalRecords = 0;
	for (int b = 0; b < count; b++)
	{
		//We run readBlock once per block in the file (number indicated by 'count')
		int n = readBlock(file, records);
		totalRecords += n;

		//hit End Of File (EOF)
		if (n < RECORDS_PER_BLOCK)
		{
			break;
		}
	}

	return totalRecords;
}

//writeBlock()
//======================================================================================
//This method writes the contents of a Records vector (max 1 block, 4096 bytes) onto 'file'
void DiskManager::writeBlock(FILE* file, const std::vector<Record>& records, int count)
{
	//check the value of 'count'
	if (count <= 0 || count > RECORDS_PER_BLOCK) return;

	//pack records into blocks (4096 bytes)
	char buffer[BLOCK_SIZE];
	std::memset(buffer, 0, BLOCK_SIZE);		//fill with zeroes

	//copy the contents of the Records vector onto the buffer
	for (int i = 0; i < count; i++)
	{
		std::memcpy(buffer + (i * Record::SIZE), records[i].data, Record::SIZE);
	}

	//Write the contents of the buffer onto the out file
	std::fwrite(buffer, 1, BLOCK_SIZE, file);
	writeCount++;
}

//writeBlocks()
//======================================================================================
//This method uses writeBlock to write all records contained in a given Record vector wonto
//a given out file
void DiskManager::writeAllRecords(FILE* file, const std::vector<Record>& records)
{
	int total = (int)records.size();	//number of records
	int offset = 0;

	//we will fit all records into one or more blocks 
	while (offset < total)
	{
		//We make sure we dont write more than 40 records per block
		int count = std::min(RECORDS_PER_BLOCK, total - offset);

		std::vector<Record> blockRecords(records.begin() + offset, records.end() + offset + count);

		writeBlock(file, records, count);
		offset += count;
	}
}

//countBlocks()
//======================================================================================
//Count how many blocks a file has
int DiskManager::countBlocks(const std::string& filePath)
{
	FILE* f = std::fopen(filePath.c_str(), "rb");	//rb = "read binary", means we open this file in reading mode
	if (!f) return 0;	//fopen returns null if file could not be opened

	//fseek moves the file pointer to a specific location within the file
	// In this case, we want to move it to the end of the file.
	//	f:			pointer to FILE object
	//	0:			offset in bytes from the position
	//	SEEK_END:	position from where the offset is added. SEEK_END is a macro for '2', 
	std::fseek(f, 0, SEEK_END);

	//ftell tells us the position of the file pointer with respect to the beginning of the file.
	//This way, we obtain the size of the file in characters (and bytes)
	long fileSize = std::ftell(f);
	std::fclose(f);

	//return how many blocks there are in the file
	return (int)((fileSize + BLOCK_SIZE - 1) / BLOCK_SIZE);
}

//countRecords()
//======================================================================================
//Count how many records a file has
int DiskManager::countRecords(const std::string& filePath)
{
	//Open file in read-mode
	FILE* f = std::fopen(filePath.c_str(), "rb");
	if (!f) return 0;

	DiskManager dm;	//method "readBlock" needs to be tied to an object because this method is static
	std::vector<Record> all;

	while (true)
	{
		int n = dm.readBlock(f, all);
		if (n == 0) break;	//exit when there are no blocks left to read

		if (n < RECORDS_PER_BLOCK) break;	//incomplete block means we've hit the end
	}
	std::fclose(f);
	return (int)all.size();
}
