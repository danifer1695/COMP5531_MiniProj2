#pragma once
#include<iostream>

int main()
{
	int a = 20;
	int* pointer = &a;

	std::cout << pointer << std::endl;
	std::cout << *pointer << std::endl;

	return 0;
}