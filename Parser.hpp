#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <iostream>

#include "Modes.hpp"
#include "HashtableT.hpp"
#include "Journal.hpp"
#include "Lista.hpp"
#include "VQueries.hpp"

static uint64_t idlist=0; 

///////////////////////////Mutexes and Condition Variable Initialization///////////////////////////

pthread_mutex_t mtxpop = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mtxx = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mtxwr = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;


//////////////////////Argument struct for passing arguments in a thread routine/////////////////////

typedef struct Arguments_t{

	uint64_t vId;
	Lista<VQueries> *v_array;
	Journal **j_array;
	HashtableT<CellT> **h_array;
	HashtableT<CellT2> **h_array2;
	ofstream *results;
	ofstream *debug;
	int subpart;

}Arguments;

using namespace std;


////////////////////////////////////////Bit Array Functions////////////////////////////////////////

bool hasOne(bool *array, uint64_t size){
	for(uint64_t i=0; i<size; i++){
		if(array[i]==1)
			return 1;
	}
	return 0;
}

bool * bitwiseAND( bool *array1, bool *array2, uint64_t size){
	for(uint64_t i=0; i<size; i++){
		array1[i] = array1[i] & array2[i];
	}
	return array1;
}


///////////////////////////////////////Processess Main Body////////////////////////////////////////


static void processTransaction(Transaction_t *t, Journal **j_array, HashtableT<CellT> **h_array,HashtableT<CellT2> **h_array2, ofstream *debug){
	int i,j,k;
	const char* reader = t->operations;
	int rel_colcount;
	JournalRecord * rec;
	CellT * cell;
	CellT2 * cell2;
	uint64_t* temparray;
	uint64_t tempkey;
	long long int pos_h, pos_j;

	for(i=0; i < t->deleteCount; i++) {
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;

		for(j=0; j< o->rowCount; j++){

			tempkey = o->keys[j];
			pos_h = h_array[o->relationId]->lastins(tempkey);


			if (pos_h != -1){
				JournalRecord * rec2 = j_array[o->relationId]->getFromOffset(pos_h);
				rec = new JournalRecord();
				rec->setCopy(rec2->getRecsize(), rec2->getRecord());
				rec->setTransactionID(t->transactionId);
				if(h_array[o->relationId]->currentdel(rec->getPrimaryKey())==0)
					rec->setdirty(1);
				pos_j = j_array[o->relationId]->insertJournalRecord(rec);
				if(rec->getdirty()!=1){
					cell = new CellT(rec->getTransactionID(),pos_j-1,-1);
					h_array[o->relationId]->insertHashRecord(rec->getPrimaryKey(), cell, 0);
					cell2 = new CellT2(pos_j-1);
					h_array2[o->relationId]->insertHashRecord(rec->getTransactionID(),cell2, 0);
				}
				
			}
		}
		
		reader += 2*sizeof(uint32_t) + (sizeof(uint64_t)*o->rowCount);
	}

	for(i=0; i < t->insertCount; i++) {
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;

		rel_colcount = j_array[o->relationId]->getColnum();
		temparray = new uint64_t[rel_colcount];

		for(j=0; j< o->rowCount; j++){

			for(k=0; k<rel_colcount; k++){
				temparray[k] = o->values[j*rel_colcount + k];
			}

			rec = new JournalRecord(t->transactionId, temparray, rel_colcount);
			pos_j = j_array[o->relationId]->insertJournalRecord(rec);
			cell = new CellT(rec->getTransactionID(),-1,pos_j-1);
			h_array[o->relationId]->insertHashRecord(rec->getPrimaryKey(), cell, 0);
			cell2 = new CellT2(pos_j-1);
			h_array2[o->relationId]->insertHashRecord(rec->getTransactionID(), cell2, 0);
		}
		reader += 2*sizeof(uint32_t) + (sizeof(uint64_t)*o->rowCount*rel_colcount);
		delete [] temparray;
	}
  
}
static void processValidationQueries(ValidationQueries_t *v, Lista<VQueries> *v_array,
									HashtableT<CellT3> **h_array, int part, ofstream *debug){

	CellT3 * cell3 = NULL;
	
	const char* reader = v->queries;
	int i, j;

	VQueries * vq = new VQueries(v->validationId, v->from, v->to, v->queryCount);
	//*debug << "ValidationQueries " << v->validationId << " [" <<v->from << ", "<< v->to << "] "<<v->queryCount << endl;

	for (i=0; i< v->queryCount; i++){

		const Query_t* q = (Query_t*)reader;
		Query * qq = new Query(q->relationId, q->columnCount);

		//*debug << "\t" << q->relationId;

		for (j=0; j< q->columnCount; j++){

			/////////////////////////part2///////////////////////////////////////

			if (part == 2){
				cell3 = new CellT3(v->from, v->to, q->columns[j].column, q->columns[j].op, q->columns[j].value);
				uint64_t pk = cell3->createPK2();
				h_array[q->relationId]->insertHashRecord(pk, cell3, 1);
				cell3 = h_array[q->relationId]->getUnique(pk, cell3);
				cell3->push_valptr(vq);
			}

			/////////////////////////part2///////////////////////////////////////

			Column * cc = new Column(q->columns[j].column, q->columns[j].op, q->columns[j].value, cell3);
			qq->setColumn(cc, j);
			//*debug << " (C"<< q->columns[j].column <<" "<< q->columns[j].op <<" "<< q->columns[j].value <<")";
		}

		//if (q->columnCount==0)
			//*debug << "ValidationQueries " << v->validationId << " [" <<v->from << ", "<< v->to << "] "<<v->queryCount << endl;

		vq->setQuery(qq, i);

		reader += 2*sizeof(uint32_t) + (sizeof(Column_t)*q->columnCount);

		//*debug << endl;

	}

	v_array->push(vq);

}


static void processFlush(Flush_t *fl, Lista<VQueries> *v_array, Journal **j_array, 
						HashtableT<CellT> **h_array, HashtableT<CellT2> **h_array2, HashtableT<CellT3> **h_array3,
						ofstream *results, ofstream* debug, uint64_t forgetnum, int part, int subpart){

	uint32_t i=0, j=0;
	static uint64_t fullsec;
	long long int start;
	VQueries* v;
	timeval t1;
	timeval t2;

	v = v_array->pop();                   //checking if head NULL

	if (v==NULL) return;

	while(v->getVid() <= fl->validationId){				//fl->validationId mexri pio validation tha ypologisw
		
		bool answer = 0;
		uint32_t queryCount = v->getQueryCount();
		
		Lista<JournalRecord> * lista = NULL;

		for (i=0; i< queryCount; i++){			//gia queries

			Query * q = v->getQuery(i);
			Column * c;
			int flag=0;

			/////////////////////////////////// Filling the List /////////////////////////////////////

			//gettimeofday(&t1, 0);
			
			uint32_t colCount = q->getColCount();

			if(subpart == 1){ //----------------------getHashJR----------------------//
				for (j=0; j< colCount; j++){
					c =  q->getColumn(j);
					if((c->getCol() == 0)&&(c->getOp() == Equal)){
						lista = h_array[q->getRelId()]->getHashJR(j_array[q->getRelId()], v->getFrom(), v->getTo(), c->getVal());
						break;
					}
				}
				if(lista != NULL){
					if(lista->getCounter()==0){
						delete lista;
						lista = NULL;
					}
				}
				if(lista == NULL){
					if (colCount == 0)
						lista = j_array[q->getRelId()]->getJournalJR3(v->getFrom(), v->getTo());
					else{
						lista = j_array[q->getRelId()]->getJournalJR2(v->getFrom(), v->getTo(),q->getColumn(0));
						flag=1;
					}
				}
			}
			else if (subpart == 2){ //----------------getTransactionIndexRecord----------------------//
				start=-1;
				start=h_array2[q->getRelId()]->getTransactionIndexRecord(v->getFrom());
				if(start>=0){
					lista = j_array[q->getRelId()]->getJournalV2(start,v->getTo());
				}
				else
					lista = j_array[q->getRelId()]->getJournalJR(v->getFrom(), v->getTo());	
			}
			else{ //-----------------------------getHashJR + getTransactionIndexRecord----------------------//
				for (j=0; j< colCount; j++){
					c =  q->getColumn(j);
					if((c->getCol() == 0)&&(c->getOp() == Equal)){
						lista = h_array[q->getRelId()]->getHashJR(j_array[q->getRelId()], v->getFrom(), v->getTo(), c->getVal());
						break;
					}
				}
				if(lista != NULL){
					if(lista->getCounter()==0){
						delete lista;
						lista = NULL;
					}
				}
				if(lista == NULL){
					start=-1;
					start=h_array2[q->getRelId()]->getTransactionIndexRecord(v->getFrom());
					if(start>=0){
						lista = j_array[q->getRelId()]->getJournalV2(start,v->getTo());
					}
					else{
						if (colCount == 0)
							lista = j_array[q->getRelId()]->getJournalJR3(v->getFrom(), v->getTo());
						else{
							lista = j_array[q->getRelId()]->getJournalJR2(v->getFrom(), v->getTo(),q->getColumn(0));
							flag=1;
						}	
					}
				}
			}

			//gettimeofday(&t2, 0);	

			//*debug << t2.tv_sec - t1.tv_sec<<"."<<t2.tv_usec - t1.tv_usec << endl;
			//fullsec += (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);
			//*debug << "             " << fullsec << endl;

			////////////////////////////////////////////////////////////////////////////////////////

			answer = 1; // answer = 1 kai an vrethei estw kai ena column = 0, tote query = 0
			
			uint64_t elemcount = lista->getCounter();
			if (elemcount==0){
				answer=0;
				flag = colCount;
			}
			
			bool * bitarray = new bool[elemcount];
			memset(bitarray, 1, elemcount*sizeof(bool));
			
			//gettimeofday(&t1, 0);

			for (j=flag; j< colCount; j++){

				c = q->getColumn(j);

				elemcount = lista->getCounter();

				uint32_t c1 = c->getCol();
				uint64_t value = c->getVal();
				Op_t op = c->getOp();

				JournalRecord * rec;
				CellT3* mycell = NULL;
				bool * colbits = NULL;

				if(part == 1){ //Ypologismos me elimination
					for(uint64_t elemptr=0; elemptr<elemcount; elemptr++){
						rec = lista->pop();
						if (rec==NULL) break;

						switch(op){
							case Equal:	if(rec->getSpecCol(c1+1)==value){ lista->push(rec); } break;
							case NotEqual: if(rec->getSpecCol(c1+1)!=value){ lista->push(rec); } break;
							case Less: if(rec->getSpecCol(c1+1)<value){ lista->push(rec); } break;
							case LessOrEqual: if(rec->getSpecCol(c1+1)<=value){ lista->push(rec); } break;
							case Greater: if(rec->getSpecCol(c1+1)>value){ lista->push(rec); } break;
							case GreaterOrEqual: if(rec->getSpecCol(c1+1)>=value){ lista->push(rec); } break;
							default: return;
						}
					}
					
					if(lista->getCounter() == 0){
						answer = 0;
						break;
					}
				}
				else if(part == 2){ //Ypologismos me ValidationIndex

					mycell = c->getPtr();
					if (mycell->get_consize()!=0){
						colbits = mycell->get_conflicts();
					}
					else{
						colbits = new bool[elemcount]; 
						for(uint64_t elemptr=0; elemptr<elemcount; elemptr++){
							rec = lista->pop();
							if (rec==NULL) break;
							switch(op){
								case Equal:	if(rec->getSpecCol(c1+1)==value){ colbits[elemptr] = 1;	}
											else{ colbits[elemptr] = 0; } lista->push(rec);	break;
								case NotEqual: if(rec->getSpecCol(c1+1)!=value){ colbits[elemptr] = 1; }
											else{ colbits[elemptr] = 0; } lista->push(rec);	break;
								case Less: if(rec->getSpecCol(c1+1)<value){ colbits[elemptr] = 1; }
											else{ colbits[elemptr] = 0; } lista->push(rec);	break;
								case LessOrEqual: if(rec->getSpecCol(c1+1)<=value){ colbits[elemptr] = 1; }
											else{ colbits[elemptr] = 0; } lista->push(rec);	break;
								case Greater: if(rec->getSpecCol(c1+1)>value){ colbits[elemptr] = 1; }
											else{ colbits[elemptr] = 0; } lista->push(rec);	break;
								case GreaterOrEqual: if(rec->getSpecCol(c1+1)>=value){ colbits[elemptr] = 1; }
											else{ colbits[elemptr] = 0; } lista->push(rec);	break;
								default: return;
							}
						}

						mycell->set_conflicts(colbits);
						mycell->set_consize(elemcount);
					}

					mycell->pop_valptr();
					if((mycell->cur_valptr()==0)&&(mycell->get_end()<=forgetnum)){
						uint64_t pk = mycell->createPK2();
						h_array3[q->getRelId()]->deleteHashRecord(pk, mycell);
					}

					bitarray = bitwiseAND(bitarray, colbits, elemcount); 
					if(!(hasOne(bitarray, elemcount))) {
						answer = 0;
						break;
					}
				}

			} //for tou column

			//gettimeofday(&t2, 0);

			//*debug << t2.tv_sec - t1.tv_sec<<"."<<t2.tv_usec - t1.tv_usec << endl;
			//fullsec += (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);
			//*debug << "             " << fullsec << endl;

			if(colCount==0){
				if(lista->getCounter()>0)
					answer=1;
				else
					answer=0;		
			}

			delete [] bitarray;
			bitarray = NULL;

			lista->popall();
			delete lista;
			lista = NULL;

			if(answer==1)
				break;

		} //for tou query

		if (queryCount==0)
			answer = 1;

		*results << answer;

		delete v;
		v = v_array->pop();
		if (v==NULL) return;	
	}
}



static void * validjob(void * args){ //Routina twn threads
	uint32_t i=0, j=0;
	static uint64_t fullsec;
	long long int start;
	VQueries* v;
	timeval t1;
	timeval t2;

	Arguments * myargs = (Arguments *) args;

	uint64_t validationId = myargs->vId;
	Lista<VQueries> *  v_array = myargs->v_array;
	Journal **j_array = myargs->j_array;
	HashtableT<CellT> **h_array = myargs->h_array;
	HashtableT<CellT2> **h_array2 = myargs->h_array2;
	ofstream *results = myargs->results;
	ofstream *debug = myargs->debug;
	int subpart = myargs->subpart;

	//---------------mutex lock/unlock on popping from job queue----------------//
	pthread_mutex_lock(&mtxpop);
	v = v_array->pop();                   //checking if head NULL
	pthread_mutex_unlock(&mtxpop);
	//--------------------------------------------------------------------------//

	if (v==NULL) pthread_exit(NULL);

	while(v->getVid() <= validationId){				//fl->validationId mexri poio validation tha ypologisw
		
		bool answer = 0;
		uint32_t queryCount= v->getQueryCount();
		
		Lista<JournalRecord> * lista = NULL;

		for (i=0; i< queryCount; i++){			//gia queries

			Query * q = v->getQuery(i);
			Column * c;
			int flag=0;


			uint32_t colCount = q->getColCount();

			if(subpart == 1){ //----------------------getHashJR----------------------//
				for (j=0; j< colCount; j++){
					c =  q->getColumn(j);
					if((c->getCol() == 0)&&(c->getOp() == Equal)){
						lista = h_array[q->getRelId()]->getHashJR(j_array[q->getRelId()], v->getFrom(), v->getTo(), c->getVal());
						break;
					}
				}
				if(lista != NULL){
					if(lista->getCounter()==0){
						delete lista;
						lista = NULL;
					}
				}
				if(lista == NULL){
					if (colCount == 0)
						lista = j_array[q->getRelId()]->getJournalJR3(v->getFrom(), v->getTo());
					else{
						lista = j_array[q->getRelId()]->getJournalJR2(v->getFrom(), v->getTo(),q->getColumn(0));
						flag=1;
					}
				}
			}
			else if (subpart == 2){ //----------------getTransactionIndexRecord----------------------//
				start=-1;
				start=h_array2[q->getRelId()]->getTransactionIndexRecord(v->getFrom());
				if(start>=0){
					lista = j_array[q->getRelId()]->getJournalV2(start,v->getTo());
				}
				else
					lista = j_array[q->getRelId()]->getJournalJR(v->getFrom(), v->getTo());	
			}
			else{ //-----------------------------getHashJR + getTransactionIndexRecord----------------------//
				for (j=0; j< colCount; j++){
					c =  q->getColumn(j);
					if((c->getCol() == 0)&&(c->getOp() == Equal)){
						lista = h_array[q->getRelId()]->getHashJR(j_array[q->getRelId()], v->getFrom(), v->getTo(), c->getVal());
						break;
					}
				}
				if(lista != NULL){
					if(lista->getCounter()==0){
						delete lista;
						lista = NULL;
					}
				}
				if(lista == NULL){
					start=-1;
					start=h_array2[q->getRelId()]->getTransactionIndexRecord(v->getFrom());
					if(start>=0){
						lista = j_array[q->getRelId()]->getJournalV2(start,v->getTo());
					}
					else{
						if (colCount == 0)
							lista = j_array[q->getRelId()]->getJournalJR3(v->getFrom(), v->getTo());
						else{
							lista = j_array[q->getRelId()]->getJournalJR2(v->getFrom(), v->getTo(),q->getColumn(0));
							flag=1;
						}	
					}
				}
			}

			answer = 1; 
			
			uint64_t elemcount = lista->getCounter();
			if (elemcount==0){
				answer=0;
				flag=1;
			}

			for (j=flag; j< colCount; j++){

				c = q->getColumn(j);

				uint32_t c1 = c->getCol();
				uint64_t value = c->getVal();
				Op_t op = c->getOp();

				JournalRecord * rec;
				
				for(uint64_t elemptr=0; elemptr<elemcount; elemptr++){
					rec = lista->pop();
					if (rec==NULL) break;

					switch(op){
						case Equal:	if(rec->getSpecCol(c1+1)==value){ lista->push(rec); } break;
						case NotEqual: if(rec->getSpecCol(c1+1)!=value){ lista->push(rec); } break;
						case Less: if(rec->getSpecCol(c1+1)<value){ lista->push(rec); } break;
						case LessOrEqual: if(rec->getSpecCol(c1+1)<=value){ lista->push(rec); } break;
						case Greater: if(rec->getSpecCol(c1+1)>value){ lista->push(rec); } break;
						case GreaterOrEqual: if(rec->getSpecCol(c1+1)>=value){ lista->push(rec); } break;
						default: pthread_exit(NULL);
					}
				}
				
				if(lista->getCounter() == 0){
					answer = 0;
					break;
				}
				

			} //for tou column

			if(colCount == 0){
				if(lista->getCounter()>0)
					answer=1;
				else
					answer=0;		
			}

			lista->popall();
			delete lista;
			lista = NULL;

			if(answer==1)
				break;
		} //for tou query

		if (queryCount==0)
			answer = 1;

		//---------------mutex lock/unlock on writing in shared file----------------//
		pthread_mutex_lock(&mtxwr);

		while(idlist < v->getVid()){
			pthread_cond_wait(&cond, &mtxwr);
		}
		idlist++;
		*results << answer;
		pthread_cond_broadcast(&cond);
		pthread_mutex_unlock(&mtxwr);
		//--------------------------------------------------------------------------//


		delete v;


		//---------------mutex lock/unlock on popping from job queue----------------//
		pthread_mutex_lock(&mtxpop);
		v = v_array->pop();
		pthread_mutex_unlock(&mtxpop);
		//--------------------------------------------------------------------------//

		if (v==NULL) 
			pthread_exit(NULL); 
	}
	
	pthread_exit(NULL); 
}



static void processFlush2(Flush_t *fl, Lista<VQueries> *v_array, Journal **j_array, 
						HashtableT<CellT> **h_array, HashtableT<CellT2> **h_array2, ofstream *results, ofstream* debug,
						int threads, int subpart){

	Arguments * myargs = new Arguments;

	myargs->vId = fl->validationId;
	myargs->v_array = v_array;
	myargs->j_array = j_array;
	myargs->h_array = h_array;
	myargs->h_array2 = h_array2;
	myargs->results = results;
	myargs->debug = debug;
	myargs->subpart = subpart;

	void * args = (void *) myargs;

	pthread_t thr[threads];

	pthread_mutex_lock(&mtxx);
	for(int i=0; i<threads; i++){
		pthread_create(&thr[i], NULL, validjob, args);
	}
	pthread_mutex_unlock(&mtxx);

	for(int i=0; i<threads; i++){
		pthread_join(thr[i], NULL);
	}

	delete myargs;
}




static void processForget(Forget_t *fo, uint64_t & frg){

	frg = fo->transactionId;

}
