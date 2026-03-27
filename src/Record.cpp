#include "../include/Record.h"

//Constructor
//======================================================================================
Record::Record()
{
	//memset: used to fill a block of memory with a specific value. In this case:
	//	data:	pointer to the start of the array
	//	0:		value to fill the array with
	//	SIZE:	defined earlier as '100' tells the function how many '0's to add 
	//	(careful! c++ does not tell you if you go beyond the bounds of the array)
	std::memset(data, 0, SIZE);
}

//Overloaded "<" operator
//======================================================================================
bool Record::operator<(const Record& other) const
{
	//memcmp: compares a given number of bytes of data
	//	data:		pointer to first element of the first array to be compared
	//	other.data:	pointer to first element of the second array to be compared
	//	SIZE:		number of bytes to compare
	//returns values:
	//	r>0: first element is greater
	//	r<0: second element is greater
	//	r=0: elements are equal
	return std::memcmp(data, other.data, SIZE) < 0;
}

//Overloaded "==" operator
//======================================================================================
bool Record::operator==(const Record& other) const
{
	return std::memcmp(data, other.data, SIZE) == 0;
}

//Overloaded ">" operator
//======================================================================================
bool Record::operator>(const Record& other) const
{
	return std::memcmp(data, other.data, SIZE) > 0;
}

//Overloaded ">" operator
//======================================================================================
bool Record::operator!=(const Record& other) const
{
	//We use our overloaded equality "==" operator
	return !(*this == other);
}

//Overloaded "<=" operator
//======================================================================================
bool Record::operator<=(const Record& other) const
{
	return !(*this > other);
}

//Overloaded ">=" operator
//======================================================================================
bool Record::operator>=(const Record& other) const
{
	return !(*this < other);
}

//getStudentID()
//======================================================================================
std::string Record::GetStudentID() const
{
	//std::string(data, 8) - what are these parameters?
	//	data + offset:	pointer to the position where to start copying
	//	8:				how many characters do we want to copy? StudentID is assigned 8 bytes, so we copy 8 chars
	std::string toReturn = std::string(data, 8);
	return toReturn;
}

//isEmpty()
//======================================================================================
bool Record::isEmpty() const
{
	//check every character in 'data'
	for (int i = 0; i < SIZE; i++)
	{
		if (data[i] != '0') return false;	//maybe '\0' (terminal character)?
	}

	return true;
}

//Overloaded insert operator
//======================================================================================
std::ostream& operator<<(std::ostream& os, const Record& r)
{
	//os: "out-stream", in this case to the console
	os << "======================================"
		<< "ID= " << r.GetStudentID()
		<< "\nFirst Name= " << r.GetFirstName()
		<< "\nLast Name= " << r.GetLastName()
		<< "\nDepartment= " << r.GetDepartment()
		<< "\nProgram Code= " << r.GetProgramCode()
		<< "\nSIN= " << r.GetSIN()
		<< "======================================\n";

	return os;
}