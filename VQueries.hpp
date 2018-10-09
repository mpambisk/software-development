#include <stdint.h>
#include "Modes.hpp"
//#include "Cells.hpp"

#ifndef VQUERIES_HPP
	#define VQUERIES_HPP

class CellT3;

//------------------------------------------------VALIDATIONS------------------------------------------------//

class Column {                             //Predicate   :      column praksh value
	uint32_t col;
	Op_t op;
	uint64_t value;
	CellT3 * hashptr;

public:
	Column();
	Column(uint32_t, Op_t, uint64_t, CellT3*);
	~Column();
	void setCol(uint32_t);
	void setOp(Op_t);
	void setValue(uint64_t);
	void setPtr(CellT3 *);
	CellT3 * getPtr();
	uint32_t getCol();
	Op_t getOp();
	uint64_t getVal();

};

//---------------------------------------------------------------------------
class Query {							//Query : relation id kai pinakas apo predicate kai megethos pinaka					
   uint32_t relationId;
   uint32_t columnCount;
   Column ** columns;

public:
	Query();
	Query(uint32_t, uint32_t);
	~Query();
	void setRelId(uint32_t);
	void setColCount(uint32_t);
	void setColumn(Column *, uint32_t);
	uint32_t getRelId();
	uint32_t getColCount();
	Column * getColumn(uint32_t);


};

//---------------------------------------------------------------------------

class VQueries {							//   Validation : id pinakas apo queries kai from kai to  kai megethos pinaka
   uint64_t validationId;
   uint64_t from;
   uint64_t to;
   uint32_t queryCount;
   Query ** queries;

public:
	VQueries();
	VQueries(uint64_t, uint64_t, uint64_t, uint32_t);
	~VQueries();
	void setVid(uint64_t);
	void setFrom(uint64_t);
	void setTo(uint64_t);
	void setQueryCount(uint32_t);
	void setQuery(Query *, uint32_t);
   	uint64_t getVid();
   	uint64_t getFrom();
   	uint64_t getTo();
   	uint32_t getQueryCount();
   	Query * getQuery(uint32_t);

};
#endif