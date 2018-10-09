#include <iostream>
#include <fstream>
#include <cstdlib>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <ctime>
#include <sys/time.h>

#include "Parser.hpp"
#include "Modes.hpp"
#include "HashtableT.hpp"
#include "Journal.hpp"
#include "Lista.hpp"
#include "VQueries.hpp"


using namespace std;

void finalPrinter(Journal ** j_array, HashtableT<CellT> ** h_array, int size){
	int i;
	cout << endl << endl;
	for(i=0; i<size; i++){
		cout << "\tRelation "<< i << endl;
		cout << "\tJournal "<< i << endl;
		j_array[i]->printJournal();
		cout << endl;
		cout << "\tHash "<< i << endl;
		h_array[i]->printHash();
	}

}

int main(int argc, char **argv) {

	uint64_t fullsec=0;

	MessageHead_t head;
	void *body = NULL;
	DefineSchema_t* DSbody;
	Transaction_t* Tbody;
	ValidationQueries_t* VQbody;
	Flush_t* FLbody;
	Forget_t* FRbody;
	uint32_t len;

	uint64_t forgetnum=0;
	
	ofstream *results = new ofstream;
  	results->open ("results.txt");

  	ofstream *debug = new ofstream;
  	debug->open ("debug.txt");

	int i;
	int jsize=0;

	Journal **journals;
	HashtableT<CellT> **hashes;
	HashtableT<CellT2> **hashes2;
	HashtableT<CellT3> **hashes3 = NULL;

	Lista<VQueries> *vlist;

	int threads = 0;
	int part = 1;
	int subpart = 1;

	if(argc != 3){
		cout << "Default to Part 1" << endl;
	}
	else{
		part = atoi(argv[1]);
		subpart = atoi(argv[2]);
		if(part==1 && subpart==1)
			cout << "Using Hash_Part1" << endl;
		else if(part==1 && subpart == 2)
			cout << "Using TransactionIndexRecord" << endl;
		else if(part==2 && subpart == 2)
			cout << "Using TransactionIndexRecord with ValidationIndex" << endl;
		else if(part==1 && subpart == 3)
			cout << "Using Hash_Part1 + TransactionIndexRecord" << endl;
		else if(part==3 && subpart == 1)
			cout << "Using Hash_Part1 Threaded" << endl;
		else if(part==3 && subpart == 2)
			cout << "Using TransactionIndexRecord Threaded" << endl;
		else if(part==3 && subpart == 3)
			cout << "Using Hash_Part1 + TransactionIndexRecord Threaded" << endl;
		else{
			cout << "Default to Part 1" << endl;
			part = 1;
		}
	}

	if(part == 3)
		threads = 4; //Gia megaluterh taxythta auksanoume ta threads se mhxanhma upshloterwn prodiagrafwn

	timeval t0;
	gettimeofday(&t0, 0);

	while(1){
		// Retrieve the message head
		if (read(0, &head, sizeof(head)) <= 0) { return -1; } // crude error handling, should never happen
		//cout << "HEAD LEN "<< head.messageLen << "\t| HEAD TYPE "<<  head.type << "\t| DESC ";
		
		// Retrieve the message body
		if (body != NULL) free(body);

		if (head.messageLen > 0 ){
			body = malloc(head.messageLen*sizeof(char));

			if (read(0, body, head.messageLen) <= 0) { printf("err");return -1; } // crude error handling, should never happen
			len-=(sizeof(head) + head.messageLen);
		}

		// And interpret it
		switch (head.type) {

			case Done: 
						
				//finalPrinter(journals, hashes, jsize); 

				timeval t3;
				gettimeofday(&t3, 0);

				for(i=0; i<jsize; i++){
					delete hashes[i];
					delete hashes2[i];
					if(part == 2)
						delete hashes3[i];
					delete journals[i];

				}
				delete [] hashes;
				delete [] hashes2;
				if(part == 2)
					delete [] hashes3;
				delete [] journals;

				vlist->deleteLista();
				delete vlist;

				results->close();
				delete results;

				debug->close();
				delete debug;

				timeval t4;
				gettimeofday(&t4, 0);

				cout<<"Deletions time: "<<((t4.tv_sec - t3.tv_sec)*1000000.00 +(t4.tv_usec - t3.tv_usec))/1000000.00 << endl;
				timeval t1;
				gettimeofday(&t1, 0);
				cout<<"Elapsed time: "<<((t1.tv_sec - t0.tv_sec)*1000000.00 +(t1.tv_usec - t0.tv_usec))/1000000.00 << endl;
				//cout<< fullsec<<endl;
				return 0;

			case DefineSchema: 
				DSbody = (DefineSchema_t*) body; 
				jsize = DSbody->relationCount;
				journals = new Journal*[DSbody->relationCount];
				hashes = new HashtableT<CellT>*[DSbody->relationCount];
				hashes2 = new HashtableT<CellT2>*[DSbody->relationCount];
				if(part == 2)
					hashes3 = new HashtableT<CellT3>*[DSbody->relationCount];
				vlist = new Lista<VQueries>;

				for(i=0; i<DSbody->relationCount; i++){
					journals[i] = new Journal(DSbody->columnCounts[i]);
					hashes[i] = new HashtableT<CellT>(4);
					hashes2[i] = new HashtableT<CellT2>(1);
					if(part == 2)
						hashes3[i] = new HashtableT<CellT3>(6);
				}

				//processDefineSchema(DSbody); 
				break;
				
			case Transaction: Tbody = (Transaction_t*) body; 
				//timeval tran;
				//gettimeofday(&tran, 0);
				processTransaction(Tbody, journals, hashes,hashes2, debug); 
				//timeval tran2;
				//gettimeofday(&tran2, 0);
				//*debug << tran2.tv_sec - tran.tv_sec<<"."<<tran2.tv_usec - tran.tv_usec << endl;
				//fullsec += (tran2.tv_sec - tran.tv_sec)*1000000 + (tran2.tv_usec - tran.tv_usec);
				break;

			case ValidationQueries: VQbody = (ValidationQueries_t*) body; 
				//timeval val;
				//gettimeofday(&val, 0);
				processValidationQueries(VQbody, vlist, hashes3, part, debug);
				//timeval val2;
				//gettimeofday(&val2, 0);
				//*debug << val2.tv_sec - val.tv_sec<<"."<<val2.tv_usec - val.tv_usec << endl;
				//fullsec += (val2.tv_sec - val.tv_sec)*1000000 + (val2.tv_usec - val.tv_usec);
				break;

			case Flush: FLbody = (Flush_t*) body; 
				//timeval fl;
				//gettimeofday(&fl, 0);

				if(part == 1 || part == 2)
					processFlush(FLbody, vlist, journals, hashes, hashes2, hashes3, results, debug, forgetnum, part, subpart);
				else
					processFlush2(FLbody, vlist, journals, hashes, hashes2, results, debug, threads, subpart);

				//timeval fl2;
				//gettimeofday(&fl2, 0);
				//*debug << fl2.tv_sec - fl.tv_sec<<"."<<fl2.tv_usec - fl.tv_usec << endl;
				//fullsec += (fl2.tv_sec - fl.tv_sec)*1000000 + (fl2.tv_usec - fl.tv_usec);
				break;

			case Forget: FRbody = (Forget_t*) body; 
				processForget(FRbody, forgetnum); break;

			default:return -1; // crude error handling, should never happen
		}

    }
 
  	return 0;
}
