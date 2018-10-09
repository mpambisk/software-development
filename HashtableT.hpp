#include <iostream>
#include <stdint.h>
#include <cmath>
#include <cstring>
#include "Lista.hpp"
#include "Modes.hpp"
#include "VQueries.hpp"
#include "Journal.hpp"
#include "Cells.hpp"


#ifndef HASHTABLET_HPP
	#define HASHTABLET_HPP

using namespace std;

#define cell_size 5
#define hash_size 1024			
#define g_depth 10


uint64_t myPow(int expo){
	uint64_t base=2;
	if (expo==0)
		return 1;
	return base<<(expo-1);
	}


//------------------------------Container functions------------------------------//

template <class T> class Container{
	T** cells;			//array (of initially defined size) of arrays of same-pk entries
	long int current;	//which bucket-cell to fill
	long int size;		//container size

public:
	Container(){
		cells = new T*[cell_size];
		current = 0; 
		size = cell_size;
		/*cout<<"A container has just been created\n"<<endl;*/
	}
	~Container(){
		for(int i=0; i<current; i++){

			delete cells[i]; 
		}

		delete [] cells;
		/*cout<<"A container has just been destroyed\n"<<endl;*/

	}
 	void printContainer(){
 		int i;
 		for(i=0;i<current;i++){
 			cells[i]->printData();
 		}
 	}

	void insertContainer(T* data, bool guard){
		if(guard){
			for(uint64_t i=0; i<current; i++){
				if(cells[i]->isSame(data))
					return;
			}
		}
		if(current==size){
			expandContainer();
		}
		cells[current] = data;
		current++;
	}

	int deleteContainerT(T* data){
		for(uint64_t i=0; i<current; i++){
			if(cells[i]->isSame(data)){
				delete cells[i];
				cells[i] = NULL;
				cells[i] = cells[current-1];
				current--;
				if(current == 0)
					return 1;
				else
					return 0;
			}
		}
		return 0;

	}

	T* getUniqueC(T* data){
		for(uint64_t i=0; i<current; i++){
			if(cells[i]->isSame(data)){
				return cells[i];
			}
		}
		return NULL;
	}

	void expandContainer(){
		T** tempcells;

		tempcells = new T*[size * 2];
		memcpy(tempcells, cells, sizeof(T*[size]));
		delete cells;
		cells = tempcells;
		size = size * 2;
	}

	long int getCurrent(){
		return current;
	}

	long int getSize(){
		return size;
	}

	T** getCellTs(){
		return cells;
	}


};



//------------------------------BucketT functions------------------------------//

template <class T> class BucketT{
	Container<T>** cont;			//array (of initially defined size) of arrays of same-pk entries
	uint64_t * pkarray;  //array of primary keys inside bucket
	int local;			//local depth
	long int current;	//which bucket-cell to fill
	long int size;		//bucket size

public:
	
	BucketT( int sz ){
		cont = new Container<T>*[sz];
		for(int i=0; i<sz; i++){
			cont[i] = new Container<T>;

		}
		pkarray = new uint64_t[sz];
		local = 0;
		size = sz;
		current = 0;
		/*cout<<"A bucket has just been created\n"<<endl;*/
	}

	~BucketT(){
		for (int i = 0; i < size; i++){
			if (cont[i] != NULL)
				delete cont[i];
		}
		delete[] cont;
		delete[] pkarray;
		/*cout<<"A bucket has just been destroyed\n"<<endl;*/
	}
 	void printBucketT(uint64_t k){
 		int i;
 		cout << "Hash-Position " << k << " - local depth " << local << endl;
 		for(i=0; i<current; i++){
 			cout << "key "<< pkarray[i] << endl;
 			cont[i]->printContainer();
 		}
		cout<<endl;
 	}

 	void setReady(uint64_t pk, Container<T>* c){
 		delete cont[current];
 		cont[current] = c;
		pkarray[current] = pk;
		current++;
 	}

 	void setNullCont(uint64_t p){
 		cont[p] = NULL;
 	}

	int getLocal(){
		return local;
	}

	void setLocal(int lcl){
		local = lcl;
	}

	long int getCurrent(){
		return current;
	}

	long int getSize(){
		return size;
	}

	uint64_t * getPkarray(){
		return pkarray;
	}

	Container<T>** getCont(){
		return cont;
	}

	bool isFull(){
		if(current==size)
			return 1;
		return 0;
	}

	bool isEmpty(){
		if(current==0)
			return 1;
		return 0;
	}

	T* getUniqueB(uint64_t pk, T* data){
		for(int i=0; i<current; i++){
			if (pkarray[i]==pk){
				return cont[i]->getUniqueC(data);
			}
		}
		return NULL;
	}

	int insertBucketT(uint64_t pk, T* data, bool guard){
		for(int i=0; i<current; i++){
			if (pkarray[i]==pk){
				cont[i]->insertContainer(data, guard);
				return 1;
			}
		}

		if (current == size){
			return 0;
		}

		cont[current]->insertContainer(data, 0);
		pkarray[current] = pk;
		current++;
		return 1;
	}

	int deleteBucketT(uint64_t pk, T* data){
		int i;
		Container<T> * tempc;
		int tempi;
		int control;

		for(i=0; i<current; i++){
			if (pkarray[i]==pk){
				control = cont[i]->deleteContainerT(data);
				if(control){
					pkarray[i] = 0;
					break;
				}
				else{
					return 0;
				}
			}
		}

		if(i == current) return 0;
		
		if(i < current-1){
			tempi = pkarray[i];
			pkarray[i] = pkarray[current-1];
			pkarray[current-1] = tempi;
			tempc = cont[i];
			cont[i] = cont[current-1];
			cont[current-1] = tempc;
		}

		current--;
		return 1;

	}

};

//------------------------------HashtableT functions------------------------------//



template <class T> class HashtableT{
	uint64_t size;
	int global;
	uint64_t half;
	BucketT<T> ** table;
	int buck_size;

public:			

	HashtableT( int sz=1 ){
		size = hash_size;
		global = g_depth;
		table = new BucketT<T>*[size];
		BucketT<T> *buck = new BucketT<T>(sz);
		for(uint64_t i=0; i<size; i++){
			table[i] = buck;
		}
		buck_size=sz;
		half=0;
		/*cout<<"A hashtable has just been created!\n";	*/
	}
	
	~HashtableT(){
		uint64_t i;
		int templocal;
		long int n;

		for(i=0; i<size; i++){
			templocal = table[i]->getLocal();
			if(templocal==global){
				delete table[i];
				table[i]=NULL;
				continue;
			}
			n = myPow(global -templocal);
			if( i >= ((n-1)*size)/n ){
				delete table[i];
			}
			table[i]=NULL;
		}

		delete [] table;

		/*cout<<"A hashtable has just been destroyed!\n";*/
	}

	uint64_t hashFunction(uint64_t key){
		return key % size;
	}



	void insertHashRecord(uint64_t pk, T* data, bool guard){
		
		uint64_t pos = hashFunction(pk);
		uint64_t oldpk;
		int control;

		control = table[pos]->insertBucketT(pk, data, guard);
		if(control)
			return;

		if(table[pos]->getLocal() == global){
			doubleHash();
		}		
		splitHash(pos);

		insertHashRecord(pk, data, guard);
	}

	void deleteHashRecord(uint64_t pk, T* data){
		uint64_t pos = hashFunction(pk);
		int control;

		control = table [pos]->deleteBucketT(pk, data);

		if (control==0)						//ean den uphrxe bges
			return;

		while(mergingCase(pos)){
			mergeHash(pos);    
			if(half==0)
				halveHash();
			if(pos >= size)
				pos = pos - size;
		}

		if(half==0)			//mallon peritto
			halveHash();
	}

	T* getUnique(uint64_t pk, T* data){
		uint64_t pos = hashFunction(pk);
		return table[pos]->getUniqueB(pk, data);

	}

	void doubleHash(){
		//cout<<"i m in double"<<endl;
		uint64_t i;
		BucketT<T> ** table2;
		table2 = new BucketT<T>*[size*2];
		global++;
		memcpy(table2,table,size*sizeof(BucketT<T>*));
		memcpy(table2+size,table,size*sizeof(BucketT<T>*));
		delete table;
		table = table2;
		half=0;
		size = size*2;	
	}

	void halveHash(){
		uint64_t i;
		BucketT<T> ** table2;
		BucketT<T> *newbuck;
		int loc;

		half = 0;
		global--;
		
		table2 = new BucketT<T>*[size/2];
		for(i=0;i<size/2;i++){
			table2[i] = table[i];
			loc=table2[i]->getLocal();
			if (loc==global)
				half++;	
			table[i] = NULL;	
			table[i+(size/2)] = NULL;	
		}
		delete[] table;
		table = table2;
		size = size/2;	

		if(half==0)
			halveHash();

	}

	void splitHash(uint64_t pos){
	//	int t1,t2;
		//t1=time(NULL);
		BucketT<T> *newbuck1 = new BucketT<T>(buck_size);
		BucketT<T> *newbuck2 = new BucketT<T>(buck_size);
		BucketT<T> *oldbuck = table[pos];

		int loc = oldbuck->getLocal();
		
		uint64_t step = myPow(loc);

		uint64_t check = pos;
		while(check >= 0){
			table[check] = newbuck1;
			if(check - step > check) // uint64 never goes < 0 it just overflows
				break;
			check -= step;
			table[check] = newbuck2;
			if(check - step > check) // uint64 never goes < 0 it just overflows
				break;
			check -= step;

		}

		check = pos + step;
		while(check < size){
			table[check] = newbuck2;
			check += step;
			if(check < size){
				table[check] = newbuck1;
				check += step;
			}
		}

		newbuck1->setLocal(loc+1);
		newbuck2->setLocal(loc+1);

		uint64_t * PKz = oldbuck->getPkarray();
		Container<T> ** Contz = oldbuck->getCont();


		for(long int i=0; i<oldbuck->getCurrent(); i++){
			uint64_t newpos = hashFunction(PKz[i]);
			table[newpos]->setReady(PKz[i], Contz[i]);
			oldbuck->setNullCont(i);
		}

		delete oldbuck;

		if(loc+1 == global)
			half += 2;
		//t2=time(NULL);
		//cout<<"time : "<<t2-t1<<endl;
	}

	void mergeHash(uint64_t pos){
		uint64_t pos2;

		pos2 = pos + (size/2);
		if (pos2 >= size) {pos2 = pos - (size/2);}

		uint64_t * PKz = table[pos2]->getPkarray();
		Container<T> ** Contz = table[pos2]->getCont();
		
		for(int i=0; i<table[pos2]->getCurrent(); i++){
			table[pos]->setReady(PKz[i], Contz[i]);
			table[pos2]->setNullCont(i);
		}

		delete table[pos2];
		table[pos2] = table[pos];

		table[pos]->setLocal(global-1);
		half -= 2;

	}

	int mergingCase(uint64_t pos){
		uint64_t pos2;
		int sum;

		pos2 = pos + (size/2);
		if (pos2 >= size) {pos2 = pos - (size/2);}

		if(table[pos]->getLocal()==global){
			sum = table[pos]->getCurrent() + table[pos2]->getCurrent();
			if (sum <= buck_size){
				return 1;
			}
		}
		return 0;
	}


	void printHash(){
		uint64_t i;
		for(i=0;i<size;i++){
			table[i]->printBucketT(i);
		}
	}



	long int bin_search(uint64_t * array, uint64_t pk,long int max){
		long int left=0;
		long int right=max-1 ;
		long int middle;
		while(left<=right){
	        middle = (left + right) / 2;
	        if (array[middle]==pk){
	            return middle;
	   		}
	        else if (array[middle]>pk){
	              right = middle - 1;
	    	}
	        else{
	             left = middle + 1;
	    	}
   		}
		return -1;
	}

	long long int lastins(uint64_t pk){
		uint64_t res;
		
		long long int offins;
		long int i, counter,j,buckcounter,temp;
		BucketT<T> * buck;
		Container<T> ** cont;
		uint64_t * temparray;
		CellT** cells;
		
		res=hashFunction(pk);
		buck=table[res];
		cont=buck->getCont();
		temparray=buck->getPkarray();
		buckcounter=buck->getCurrent();
		temp=bin_search(temparray,pk,buckcounter);
		if (temp==-1)
			return -1;
		cells=cont[temp]->getCellTs();
		counter=cont[temp]->getCurrent();
		for(i=counter-1;i>=0;i--){
			offins=cells[i]->get_offset_ins();
			if(offins!=-1)
				return offins;
		}
		return -1;
	}


int currentdel(uint64_t pk){
	uint64_t res;
	long long int offdel;
	long int i, counter,j,buckcounter,temp;
	BucketT<T> * buck;
	Container<T> ** cont;
	uint64_t * temparray;
	CellT** cells;
	
	res=hashFunction(pk);
	buck=table[res];
	cont=buck->getCont();
	temparray=buck->getPkarray();
	
	buckcounter=buck->getCurrent();
	temp=bin_search(temparray,pk,buckcounter);
	if (temp==-1)
		return -1;
	cells=cont[temp]->getCellTs();
	counter=cont[temp]->getCurrent()-1;

	offdel = cells[counter]->get_offset_del();
	if(offdel!=-1)
		return 0;
	return -1;
}

int currentins(uint64_t pk){
	uint64_t res;
	long long int offdel;
	long long int offins;
	long int i, counter,j,buckcounter,temp;
	BucketT<T> * buck;
	Container<T> ** cont;
	uint64_t * temparray;
	CellT** cells;
	
	res=hashFunction(pk);
	buck=table[res];
	cont=buck->getCont();
	temparray=buck->getPkarray();
	buckcounter=buck->getCurrent();

	temp=bin_search(temparray,pk,buckcounter);
	if (temp==-1)
		return -1;
	cells=cont[temp]->getCellTs();
	counter=cont[temp]->getCurrent()-1;
	offins=cells[counter]->get_offset_ins();
	if(offins!=-1)
		return 0;
	return -1;
}

	


Lista<JournalRecord> * getHashJR(Journal * jour, long long int start, long long int end, uint64_t pk){
    Lista<JournalRecord> * lista = new Lista<JournalRecord>;
    JournalRecord * temp;
    uint64_t pos;
    long int left=0;
    long int right;
    long int curr;
    long int ptr=-1;
    long int j,buckcounter,tempj=0;
    uint64_t * temparray;

    BucketT<T> * buck;
    Container<T> ** cont;
    CellT** cellarray;

    pos = hashFunction(pk);
    buck=table[pos];
    buckcounter=buck->getCurrent();
   
    temparray=buck->getPkarray();
   /* for(j=0;j<buckcounter;j++){
    	if(temparray[j]==pk){
    		tempj=j;
    		break;
    	}
    }*/
    
    tempj=bin_search(temparray,pk,buckcounter);

    if (tempj==-1)
    	tempj=0;
    cont=buck->getCont();
    cellarray = cont[tempj]->getCellTs();
    curr = cont[tempj]->getCurrent();
    right = curr-1;

    while(left<=right){
        long int middle = (left + right) / 2;
        if ((cellarray[middle]->get_trans_id() >= start)&&(cellarray[middle]->get_trans_id()<= end)){
            //cout<<"if"<<endl;
            ptr=middle;
            break;
   		}
        else if (cellarray[middle]->get_trans_id()>end){
            //cout<<"elif"<<endl;
              right = middle - 1;
    	}
        else{
            //cout<<"else"<<endl;
             left = middle + 1;
    	}
    }

        if(ptr!=-1){
            long long int del;
            long long int ins;
            
            if((del=cellarray[ptr]->get_offset_del())!=-1){
            	temp = jour->getFromOffset(del);
            	if(temp->getdirty()!=1)
					lista->push(temp);
            }
            if((ins=cellarray[ptr]->get_offset_ins())!=-1){
				temp = jour->getFromOffset(ins);
            	if(temp->getdirty()!=1)
					lista->push(temp);
			}

            left=ptr-1;
            right=ptr+1;
            while(left>=0){
                if(cellarray[left]->get_trans_id()>= start){
                    if((del=cellarray[left]->get_offset_del())!=-1){
                            temp = jour->getFromOffset(del);
                        	if(temp->getdirty()!=1)
								lista->push(temp);
                    }
                    if((ins=cellarray[left]->get_offset_ins())!=-1){
                    	temp = jour->getFromOffset(ins);
                    	if(temp->getdirty()!=1)
							lista->push(temp);
                    }        
                }
                else
                    break;
                left--;
            }
            while(right<curr){
                if(cellarray[right]->get_trans_id()<= end){
                    if((del=cellarray[right]->get_offset_del())!=-1){
						temp = jour->getFromOffset(del);
                    	if(temp->getdirty()!=1)
							lista->push(temp);  
                    }
                    if((ins=cellarray[right]->get_offset_ins())!=-1){
						temp = jour->getFromOffset(ins);
                    	if(temp->getdirty()!=1)
							lista->push(temp);
					}        
                }
                else
                    break;
                right++;
            }
        }

    
    return lista;
}


long long int getTransactionIndexRecord(uint64_t key ){
	uint64_t res;
	res = hashFunction(key);
	BucketT<T> * buck;
    Container<T> ** cont;
    uint64_t * temparray;
	CellT2** cells;
    buck=table[res];
    cont=buck->getCont();
	temparray=buck->getPkarray();

	if(buck->getCurrent()==0)
		return -1;
	if(temparray[0]!=key)
		return -1;

	cells = cont[0]->getCellTs();
	
	return cells[0]->get_data();
}



};

	#endif