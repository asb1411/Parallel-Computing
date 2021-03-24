#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <ctype.h>
#include <mpi.h>

/*

Q1: File Search in Parallel

Ameya Bhalerao  2017A3PS0263P

Abhinav Tuli    2017A7PS0048P

*/

int npes, rank;
int fsize, myf, pol, myl, newlsize, ln;
double b,a;
FILE *fp;

int main(int argc, char ** argv)
{

    MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &npes);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	// If file name not entered, abort

	if(argc==1){
        if(rank==0){
            printf("File name not entered. Aborting...\n\n");
        }
        MPI_Finalize();
        return 0;
	}

	// If no keywords/operator or in case of invalid operator found, abort

	if(argc==2){
        if(rank==0){
            printf("No keywords enetered to search. Aborting...\n\n");
        }
        MPI_Finalize();
        return 0;
	}
	if(argc==3){
        if(rank==0){
            printf("No keywords enetered to search. Aborting...\n\n");
        }
        MPI_Finalize();
        return 0;
	}
	if(argc>=3){
        char* s=argv[2];
        for(int i=strlen(s)-1;i>=0;i--){
            s[i]=tolower(s[i]);
        }
        int f=strcmp(s,"and");
        int ff=strcmp(s,"or");
        if(f!=0 && ff!=0 && rank==0){
            printf("Invalid operator, enter wither AND or OR. Aborting...\n\n");
            MPI_Finalize();
            return 0;
        }
        if(f!=0 && ff!=0){
            MPI_Finalize();
            return 0;
        }
	}

	// Start time
	if(rank==0){
        a=MPI_Wtime();
	}

	newlsize=sizeof("a\n")-1;

	// File opening and handling

	char* filenm=argv[1];

	fp=fopen(filenm,"r");

    if(fp==NULL){
        if(rank==0){
            printf("File not found. Aborting...\n\n");
        }
        return 0;
    }

    // File size
    fseek(fp,0,SEEK_END);
    fsize=ftell(fp);

    // Dividing file size among processes
    myf=fsize/npes;

    pol=myf;

    myf*=rank;

    int myl=0;

    // Seek the file untill the process finds a new line
    fseek(fp,myf,SEEK_SET);
    if(rank!=0){
        for(;;){
            char i=getc(fp);
            if(i=='\n') break;
            //if(i==' ' || i=='\n' || i=='\t' || i==EOF) break;
        }
    }

    int c=0;

    // Storing word line number
    int wordl[argc-3];

    // Storing word line starting address (to be retrieved later to display)
    int worda[argc-3];

    for(int i=0;i<argc-3;i++){
        wordl[i]=worda[i]=INT_MAX;
    }

    if(rank==npes-1) c=fsize-pol*rank;

    // Storing each word in iteration
    char s[256];

    // Start position of the current line
    ln=ftell(fp);

    // Looping through the entire file

    for(;;){

        if(fscanf(fp,"%s",s)!=1) break;

        // Free the word of any punctuation marks, special characters at the end, etc.
        for(int i=strlen(s)-1;i>=0;i--){
            s[i]=tolower(s[i]);
        }

        /*int ind=0;
        for(int i=0;i<strlen(s);i++){
            if(s[i]>='a' && s[i]<='z'){
                ind =i;
                break;
            }
        }

        for(int i=ind;i<strlen(s);i++){
            s[i-ind]=s[i];
        }
        s[strlen(s)]='\0';*/

        for(int i=strlen(s)-1;i>=0;i--){
            if(s[i]>='a' && s[i]<='z') break;
            s[i]='\0';
        }
        c+=strlen(s);

        // Compare the word to the word list

        for(int i=3;i<argc;i++){
            if(strcasecmp(s,argv[i])==0){
                wordl[i-3]=fmin(myl,wordl[i-3]);
                worda[i-3]=fmin(ln,worda[i-3]);
            }
        }

        int o=0;
        char i;

        // Find the next line...this takes care of multiple newline characters
        for(;;){

            i=fgetc(fp);

            if(i==' ' || i=='\t') break;

            // Found next non-blank line
            if(i!='\n'){
                fseek(fp,-1,SEEK_CUR);

                // Adjust the next line beginning address
                ln=ftell(fp);
                break;
            }

            // This is just newline character or blank line
            if(i=='\n'){
                // Increment line count
                myl++;

                //fseek(fp,-1,SEEK_CUR);
                ln=ftell(fp);

                // Check for termination
                if(rank==npes-1){
                    if(ftell(fp)>=fsize) {o=1;break;}
                    continue;
                }
                if(ftell(fp)>=pol*(rank+1)) {o=1;break;}
            }
        }
        if(o) break;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* Do a partial sum with Scan function so that we get the
        line number of a particular line. Each processor has
        its local line number, which is converted to the global
        line number after this step.
    */
    int y=0;
    MPI_Scan(&myl,&y,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

    for(int i=3;i<argc;i++){
        if(wordl[i-3]!=INT_MAX) wordl[i-3]+=(y-myl);
    }

    // Find first occurance of the word

    if( rank == 0)
        MPI_Reduce(MPI_IN_PLACE,wordl,argc-3,MPI_INT,MPI_MIN,0,MPI_COMM_WORLD);
    else
        MPI_Reduce(wordl,wordl,argc-3,MPI_INT,MPI_MIN,0,MPI_COMM_WORLD);

    if( rank == 0)
        MPI_Reduce(MPI_IN_PLACE,worda,argc-3,MPI_INT,MPI_MIN,0,MPI_COMM_WORLD);
    else
        MPI_Reduce(worda,worda,argc-3,MPI_INT,MPI_MIN,0,MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

    if(rank==0){

        // Parallel runtime

        b=MPI_Wtime()-a;
        printf("\nParallel search time with %d processors: %f\n\n",npes,b);

        // Check AND/OR conditions

        int o=0;
        for(int i=0;i<argc-3;i++){
            if(wordl[i]!=INT_MAX) o++;
        }

        // For AND operator
        if(strcasecmp(argv[2],"and")==0){

            if(o!=argc-3){
                printf("AND combination of the given words not found.");
                MPI_Finalize();
                return 0;
            }

            // Print the word, word line, etc.
            for(int i=3;i<argc;i++){

                printf("- Word \"%s\" found at Line %d:\n\t=> ",argv[i],wordl[i-3]+1);

                // Go to the word line
                fseek(fp,worda[i-3],SEEK_SET);
                int wpos=INT_MAX, lpos=0;

                // Find word position
                for(;;){
                    if(fscanf(fp,"%s",s)!=1) break;
                    char s1[strlen(s)+3];

                    for(int i=strlen(s1)-1;i>=0;i--) s1[i]='\0';
                    for(int i=strlen(s)-1;i>=0;i--){
                        if(s1[i]==' ' || s1[i]=='\n' || s1[i]=='\t') break;
                        s1[i]=tolower(s[i]);
                    }

                    for(int i=strlen(s1)-1;i>=0;i--){
                        if(s1[i]>='a' && s1[i]<='z') break;
                        s1[i]='\0';
                    }

                    if(strcasecmp(s1,argv[i])==0){
                        wpos=fmin(wpos,lpos);
                    }
                    lpos+=strlen(s)+1;

                    printf("%s",s);
                    char i=getc(fp);
                    printf("%c",i);

                    if(i=='\n' || i==EOF) break;
                }
                printf("- at position %d\n\n",wpos);
            }
        }

        // For OR operator
        if(strcasecmp(argv[2],"or")==0){

            if(o<1){
                printf("OR combination of the given words not found.");
                MPI_Finalize();
                return 0;
            }

            // Print word, word line, etc.
            for(int i=3;i<argc;i++){

                if(worda[i-3]==INT_MAX || wordl[i-3]==INT_MAX) continue;

                printf("- Word \"%s\" found at Line %d:\n\t=> ",argv[i],wordl[i-3]+1);

                // Go to word line
                fseek(fp,worda[i-3],SEEK_SET);
                int wpos=INT_MAX, lpos=0;

                // Find word position
                for(;;){
                    if(fscanf(fp,"%s",s)!=1) break;
                    char s1[strlen(s)+3];

                    for(int i=strlen(s1)-1;i>=0;i--) s1[i]='\0';
                    for(int i=strlen(s)-1;i>=0;i--){
                        if(s1[i]==' ' || s1[i]=='\n' || s1[i]=='\t') break;
                        s1[i]=tolower(s[i]);
                    }

                    for(int i=strlen(s1)-1;i>=0;i--){
                        if(s1[i]>='a' && s1[i]<='z') break;
                        s1[i]='\0';
                    }

                    if(strcasecmp(s1,argv[i])==0){
                        wpos=fmin(wpos,lpos);
                    }
                    lpos+=strlen(s)+1;

                    printf("%s",s);
                    char i=getc(fp);
                    printf("%c",i);

                    if(i=='\n' || i==EOF) break;
                }
                printf("- at position %d\n\n",wpos);
                if(wordl[i-3]!=INT_MAX){
                    MPI_Finalize();
                    return 0;
                }
            }
        }
    }

    MPI_Finalize();
}
