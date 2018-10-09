#include <stdint.h>
#include <cstring>

#include "Modes.hpp"
#include "VQueries.hpp"
#include "Lista.hpp"

#ifndef CELLS_HPP
	#define CELLS_HPP

class CellX{
	int a;
	int b;
	int c;

	public:
		CellX();
		CellX(int, int, int);
		~CellX();
		void printData();
};

class CellT{
	uint64_t trans_id;			//transaction id
	long long int offset_del;			//offset for deletion record
	long long int offset_ins;			//offset for insertion record
	
	public:
		CellT();
		CellT(uint64_t, long long int, long long int);		
		~CellT();
		uint64_t get_trans_id();
		long long int get_offset_del();
		long long int get_offset_ins();
		void set_trans_id(uint64_t);
		void set_offset_del(long long int);
		void set_offset_ins(long long int);
		void printData();
        bool isSame(CellT*);
};

class CellT2{
	long long int data;
	
	public:
		CellT2();
		CellT2(long long int);			
		~CellT2();
		long long int get_data();
		void set_data(long long int);
		void printData();
        bool isSame(CellT2*);
};

class CellT3{
    bool * conflicts;
    uint64_t consize;
    Lista<VQueries> * ptrs;
    uint64_t start;
    uint64_t end;
    uint32_t col;
    Op_t op;
    uint64_t value;

    public:
        CellT3();
        CellT3(uint64_t, uint64_t, uint32_t, Op_t, uint64_t);
        ~CellT3();

        void set_conflicts(bool *);
        void set_consize(uint64_t );
        void set_start(uint64_t );
        void set_end(uint64_t );
        void set_value(uint64_t );
        void set_col(uint32_t );
        void set_op(Op_t );
        Op_t get_op();
        uint64_t get_start();
        uint64_t get_end();
        uint64_t get_value();
        uint32_t get_col();
        bool* get_conflicts();
        uint64_t get_consize();
        void push_valptr(VQueries *);
        VQueries * pop_valptr();
        int cur_valptr();

        Lista<VQueries> * get_ptrs();

        void printData();
        bool isSame(CellT3*);

        uint64_t createPK();
        uint64_t createPK2();
        uint64_t createPK3();
};

#endif