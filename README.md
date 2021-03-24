# Parallel-Computing
Parallel computing projects in MPI and C

## Parallel File Search

A parallel file search algorithm designed using C and MPI to parallelly search a large file for AND/OR combination of the given words.
The file name and the wordlist are taken as command line arguments.

Compile: mpicc search_parallel.c

Execute: mpiexec -n N search_parallel.exe "filename.txt" "AND/OR" word1 word2...

N = number of proessors/workers desired
