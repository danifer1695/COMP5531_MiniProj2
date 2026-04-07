The program requires a C++17-compliant compiler due to the use of the standard filesystem library. To compile the program, open the command console inside the “src” folder within the root folder.  Inside, files main.cpp and main.exe can be found. Once in this directory, enter the following in the command console:

g++ -std=c++17 main.cpp  -o main
or
clang++ -std=c++17 main.cpp -o main 

Once compiled, ensure that the input files T1_15000, T2_15000, T1_240000, T2_240000, T1_960000 and T2_960000 are present in folder “data” found at the project root. The program can then be executed using:

./main [Arg1] [Arg2] [Arg3]

[Arg1]: tells the program whether the user wants it to clean up the temp files it creates or not. Accepts “cleanup” or “no-cleanup” as arguments.
[Arg2]: tells the program whether the user wants it to log its progress or not. Accepts “log” or “no-log” as arguments.
[Arg3]: tells the program the size of the testing load the user wants it to perform. Accepts arguments:
	-“small”: tests with 30000 input records total.
	-“medium”: test with 30000 and 240000 input records.
	-“large”: test with 30000, 240000 and 960000 input records.

Example: ./main cleanup log large - Program will be tested using the largest data load, it will clean its temporary files up after use, and will log its progress in more detail.

Upon execution, the program will automatically generate intermediate run files within the runs/ directory and produce the final bag union output in output/bag_union_output.txt. The console will display progress logs for each phase of the algorithm , including run generation and merge passes (this can be turned off with argument “no-log”).