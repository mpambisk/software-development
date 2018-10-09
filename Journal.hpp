#include <stdint.h>
#include "Lista.hpp"
#include "VQueries.hpp"

#ifndef JOURNAL_HPP
	#define JOURNAL_HPP

class JournalRecord{
	uint64_t * record;
	int recsize;
	bool dirty;
	public:
		JournalRecord(uint64_t, uint64_t *, int);
		JournalRecord();

		~JournalRecord();
		void printData();
		uint64_t getPrimaryKey();
		uint64_t getTransactionID();
		uint64_t getSpecCol(uint32_t);
		bool getdirty();
		void setdirty(bool);
		void setTransactionID(uint64_t);
		//void setRecord(uint64_t *);
		void setRecsize(int);
		void setCopy(int, uint64_t *);
		int getRecsize();
		uint64_t * getRecord();
};



class Journal{
	JournalRecord ** journal;
	int colnum;
	long long int maxsize;
	long long int current;	
	public:
		Journal();
		Journal(int);
		~Journal();
		void printJournal();
		long long int insertJournalRecord(JournalRecord*);
		int increaseJournal();	
		int getColnum();
		Lista<JournalRecord> * getJournalJR(uint64_t,uint64_t);	
		Lista<JournalRecord> * getJournalV2(long long int,uint64_t);
		Lista<JournalRecord> * getJournalJR2(uint64_t, uint64_t, Column* );
		Lista<JournalRecord> * getJournalJR3(uint64_t ,uint64_t );

		//JournalRecord * findLastEntry(uint64_t);
		JournalRecord * getFromOffset(long long int);
};

#endif