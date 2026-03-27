#pragma once

//Include required libraries
#include <cstring>
#include <string>
#include <iostream>

// Fixed-width record layout (100 bytes total):
//   Student ID:   bytes  0 -  7  (8 bytes,  int stored as chars)
//   First Name:   bytes  8 - 17  (10 bytes, char)
//   Last Name:    bytes 18 - 27  (10 bytes, char)
//   Department:   bytes 28 - 30  (3 bytes,  int stored as chars)
//   Program Code: bytes 31 - 33  (3 bytes,  int stored as chars)
//   SIN Number:   bytes 34 - 42  (9 bytes,  int stored as chars)
//   Address:      bytes 43 - 99  (57 bytes, char)

struct Record
{
	static constexpr int SIZE = 100;	//maximum size of each tuple, in characters (1 char = 1 byte)

	char data[SIZE];	//actual content of the tuple, formated as a C-style string (an array of chars)

	int a = 4;
	int b = 8;
	//Constructor declarations
	//======================================================================================
	Record();
	 
	//Function declarations
	//======================================================================================
	//Overridden operators
	bool operator<	(const Record& other) const;
	bool operator==	(const Record& other) const;
	bool operator>	(const Record& other) const;
	bool operator!=	(const Record& other) const;
	bool operator<=	(const Record& other) const;
	bool operator>=	(const Record& other) const;
	friend std::ostream& operator<<(std::ostream& os, const Record& r);

	//Getters
	std::string GetStudentID() const;	//definition & explanation in source file
	std::string GetFirstName() const	{ return std::string(data + 8, 10); }
	std::string GetLastName() const		{ return std::string(data + 18, 10); }
	std::string GetDepartment() const	{ return std::string(data + 28, 3); }
	std::string GetProgramCode() const	{ return std::string(data + 31, 3); }
	std::string GetSIN() const			{ return std::string(data + 34, 9); }
	std::string GetAddress() const		{ return std::string(data + 43, 57); }

	//Return the entire data as a string
	std::string toString() const		{ return std::string(data, SIZE); }

	//Check if empty
	bool isEmpty() const;

};