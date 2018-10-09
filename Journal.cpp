#include <iostream>
#include <cstring>
#include "Lista.hpp"
#include "Journal.hpp"


#define init_size 3

using namespace std;

//------------------------------JournalRecord------------------------------//

JournalRecord::JournalRecord(uint64_t t_id, uint64_t * array, int size){
	int i;
	recsize = size+1;
	dirty=0;
	record = new uint64_t[recsize];
	record[0] = t_id;
	//for(i=1;i<(size+1);i++)
	//	record[i]=array[i-1];
	memcpy(record+1,array,size*sizeof(uint64_t));
	//cout<<"A JournalRecord has just been created!!\n"<<endl;
}

JournalRecord::JournalRecord(){
	int i;
	recsize = 2;
	dirty = 0;
	record = NULL;
	//cout<<"A JournalRecord EMPTY has just been created!!\n"<<endl;
}

JournalRecord::~JournalRecord(){
	delete [] record ;
	//cout<<"A JournalRecord has just been destroyed!!\n"<<endl;
}

void JournalRecord::printData(){
	int i;
	for(i=0;i<recsize;i++)
		cout<<record[i]<<"     ";
	cout<<endl;
} 

uint64_t JournalRecord::getPrimaryKey(){
	return record[1];
}

uint64_t JournalRecord::getTransactionID(){
	return record[0];
}

uint64_t JournalRecord::getSpecCol(uint32_t i){
	return record[i];
}

bool JournalRecord::getdirty(){
	return dirty;
}

void JournalRecord::setdirty(bool b){
	dirty=b;
}

void JournalRecord::setTransactionID(uint64_t tid){
	record[0] = tid;
}

void JournalRecord::setRecsize(int size){
	recsize = size;
}

void JournalRecord::setCopy(int size, uint64_t *eggr){
	recsize = size;
	int i;
	record=new uint64_t[recsize];
	//for(i=0; i<recsize; i++){
	//	record[i] = eggr[i];
	memcpy(record,eggr,recsize*sizeof(uint64_t));
	//}	
}

int JournalRecord::getRecsize(){
	return recsize;
}

uint64_t * JournalRecord::getRecord(){
	return record;
}



//------------------------------Journal------------------------------//

Journal::Journal(){
	long long int i;
	maxsize = init_size;
	colnum = 0;
	current = 0;
	journal = new JournalRecord*[init_size];
	//cout << "A Journal has just been created!!\n" << endl;
}

Journal::Journal(int col){
	long long int i;
	maxsize = init_size;
	colnum = col;
	current = 0;
	journal = new JournalRecord*[init_size];
	//cout << "A Journal has just been created with "<< colnum <<" columns!\n" << endl;
}

Journal::~Journal(){
	long long int i;
	for(i=0;i<current;i++)
		delete journal[i];
	delete[] journal;
	//cout<<"A Journal has just been destroyed!!\n"<<endl;
}

void Journal::printJournal(){
	long long int i=0;
	for(i=0;i<current;i++){
		journal[i]->printData();
		cout<<endl;			
	}
}

long long int Journal::insertJournalRecord(JournalRecord * jrec){
	int i;
	if(current==maxsize)
		i=increaseJournal();
	journal[current]=jrec;
	current++;
	return current;
}

int Journal::increaseJournal(){
	//cout<<"The journal is increasing"<<endl;
	
	JournalRecord ** journal2;
	long long int i;
	journal2=new JournalRecord*[maxsize*2];
	memcpy(journal2,journal,sizeof(JournalRecord*[maxsize]));

	delete[] journal;
	journal=journal2;
	maxsize=maxsize*2;
	//cout<<"the new max size is: "<<maxsize<<endl;
	//cout<<"the current is: "<<current<<endl;
	return 0;
}

int Journal::getColnum(){
	return colnum;
}

JournalRecord * Journal::getFromOffset(long long int offset){
	return journal[offset];

}

Lista<JournalRecord> * Journal::getJournalJR(uint64_t rstart,uint64_t rend){
	Lista<JournalRecord> *lista = new Lista<JournalRecord>;

	long long int left=0,right=current-1;	
	long long int jptr=-1;						//binary search 
	long long int middle;
	while(left<=right){
		middle = (left + right) / 2;
        if ((journal[middle]->getTransactionID()>= rstart)&&(journal[middle]->getTransactionID()<= rend)){
        	jptr=middle;
			break;
		}
    	else if (journal[middle]->getTransactionID()>rend){
		  	right = middle - 1;
		}
    	else{
         	left = middle + 1;
        }
	}
	if(jptr!=-1){
		if(journal[jptr]->getdirty()!=1)
			lista->push(journal[jptr]);

		left=jptr-1;
		right=jptr+1;
		
		if(left>=0){
			while(journal[left]->getTransactionID()>=rstart){
				if(journal[left]->getdirty()!=1)
					lista->push(journal[left]);
				left--;
				if(left<0)
					break;
			}
		}
		if(right<current){
			while(journal[right]->getTransactionID()<=rend){
				if(journal[right]->getdirty()!=1)
						lista->push(journal[right]);
				right++;
				if(right>=current)
					break;
			}
		}
	}

	return lista;
}

Lista<JournalRecord> * Journal::getJournalJR3(uint64_t rstart,uint64_t rend){
	Lista<JournalRecord> *lista = new Lista<JournalRecord>;

	long long int left=0,right=current-1;	
	long long int jptr=-1;						//binary search 
	long long int middle;
	while(left<=right){
		middle = (left + right) / 2;
        if ((journal[middle]->getTransactionID()>= rstart)&&(journal[middle]->getTransactionID()<= rend)){
        	jptr=middle;
			break;
		}
    	else if (journal[middle]->getTransactionID()>rend){
		  	right = middle - 1;
		}
    	else{
         	left = middle + 1;
        }
	}
	if(jptr!=-1){
		if(journal[jptr]->getdirty()!=1){
			lista->push(journal[jptr]);
			return lista;
		}

		left=jptr-1;
		right=jptr+1;
		
		if(left>=0){
			while(journal[left]->getTransactionID()>=rstart){
				if(journal[left]->getdirty()!=1){
					lista->push(journal[left]);
					return lista;
				}
				left--;
				if(left<0)
					break;
			}
		}
		if(right<current){
			while(journal[right]->getTransactionID()<=rend){
				if(journal[right]->getdirty()!=1){
					lista->push(journal[right]);
					return lista;
				}
				right++;
				if(right>=current)
					break;
			}
		}
	}

	return lista;
}

Lista<JournalRecord> * Journal::getJournalV2(long long int start,uint64_t last_tr){
	long long int i=start;
	uint64_t tr_id;
	Lista<JournalRecord> *lista = new Lista<JournalRecord>;
	tr_id=journal[i]->getTransactionID();
	while(tr_id<=last_tr){
		if(journal[i]->getdirty()!=1)
			lista->push(journal[i]);
		i++;
		if(i==current)
			break;
		tr_id=journal[i]->getTransactionID(); 
	}
	return lista;
}



Lista<JournalRecord> * Journal::getJournalJR2(uint64_t rstart,uint64_t rend,Column* column){
	Lista<JournalRecord> *lista = new Lista<JournalRecord>;
	JournalRecord * rec; 
	uint32_t c1 = column->getCol();
	uint64_t value = column->getVal();
	Op_t op = column->getOp();
	long long int left=0,right=current-1;	
	long long int jptr=-1;						//binary search 
	long long int middle;
	while(left<=right){
		middle = (left + right) / 2;
        if ((journal[middle]->getTransactionID()>= rstart)&&(journal[middle]->getTransactionID()<= rend)){
        	jptr=middle;
			break;
		}
    	else if (journal[middle]->getTransactionID()>rend){
		  	right = middle - 1;
		}
    	else{
         	left = middle + 1;
        }
	}
	if(jptr!=-1){									//an brethike
		if(journal[jptr]->getdirty()!=1){
			rec=journal[jptr];
			
			switch(op){
				case Equal:	if(rec->getSpecCol(c1+1)==value){ lista->push(rec); } break;
				case NotEqual: if(rec->getSpecCol(c1+1)!=value){ lista->push(rec); } break;
				case Less: if(rec->getSpecCol(c1+1)<value){ lista->push(rec); } break;
				case LessOrEqual: if(rec->getSpecCol(c1+1)<=value){ lista->push(rec); }	break;
				case Greater: if(rec->getSpecCol(c1+1)>value){ lista->push(rec); } break;
				case GreaterOrEqual: if(rec->getSpecCol(c1+1)>=value){ lista->push(rec); } break;
			}
		}

		left=jptr-1;
		right=jptr+1;
		
		if(left>=0){
			while(journal[left]->getTransactionID()>=rstart){
				if(journal[left]->getdirty()!=1){
					rec=journal[left];
					switch(op){
						case Equal:	if(rec->getSpecCol(c1+1)==value){ lista->push(rec); } break;
						case NotEqual: if(rec->getSpecCol(c1+1)!=value){ lista->push(rec); } break;
						case Less: if(rec->getSpecCol(c1+1)<value){ lista->push(rec); } break;
						case LessOrEqual: if(rec->getSpecCol(c1+1)<=value){ lista->push(rec); }	break;
						case Greater: if(rec->getSpecCol(c1+1)>value){ lista->push(rec); } break;
						case GreaterOrEqual: if(rec->getSpecCol(c1+1)>=value){ lista->push(rec); } break;
					}
				}
				left--;
				if(left<0)
					break;
			}
		}
		if(right<current){
			while(journal[right]->getTransactionID()<=rend){
				if(journal[right]->getdirty()!=1){
						rec=journal[right];
						
						switch(op){
							case Equal:	if(rec->getSpecCol(c1+1)==value){ lista->push(rec); } break;
							case NotEqual: if(rec->getSpecCol(c1+1)!=value){ lista->push(rec); } break;
							case Less: if(rec->getSpecCol(c1+1)<value){ lista->push(rec); } break;
							case LessOrEqual: if(rec->getSpecCol(c1+1)<=value){ lista->push(rec); }	break;
							case Greater: if(rec->getSpecCol(c1+1)>value){ lista->push(rec); } break;
							case GreaterOrEqual: if(rec->getSpecCol(c1+1)>=value){ lista->push(rec); } break;
						}
				}
				right++;
				if(right>=current)
					break;
			}
		}
	}

	return lista;

}