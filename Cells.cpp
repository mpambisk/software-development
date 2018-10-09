#include <iostream>
#include "Cells.hpp"

using namespace std;

//////////////////////////////// CellX /////////////////////////////////

CellX::CellX(){
	a=0, b=0; c=0;
}
CellX::CellX(int x,int y,int z){
	a=x; b=y; c=z;
}
CellX::~CellX(){}

void CellX::printData(){cout<<"["<< a << "-" << b << "-" << c <<"]"<<endl;}



//////////////////////////////// CellT /////////////////////////////////

CellT::CellT(){/*cout<<"A cell has just been created\n"<<endl;*/}	
CellT::CellT(uint64_t tr,long long int del,long long int ins){trans_id=tr; offset_del=del; offset_ins=ins;}		
CellT::~CellT(){/*cout<<"A cell has just been destroyed\n"<<endl;*/}
uint64_t CellT::get_trans_id(){return trans_id;}
long long int CellT::get_offset_del(){return offset_del;}
long long int CellT::get_offset_ins(){return offset_ins;}
void CellT::set_trans_id(uint64_t trid){trans_id=trid;}
void CellT::set_offset_del(long long int del){offset_del=del;}
void CellT::set_offset_ins(long long int ins){offset_ins=ins;}
void CellT::printData(){
	cout<<"trid: "<<trans_id<<"  del: "<<offset_del<<"  ins: "<<offset_ins<<endl;
}		
bool CellT::isSame(CellT * data){ return 0; }




//////////////////////////////// CellT2 /////////////////////////////////

CellT2::CellT2(){cout<<"A cell2 has just been created\n"<<endl;}
CellT2::CellT2(long long int d){data=d;}			
CellT2::~CellT2(){/*cout<<"A cell2 has just been destroyed\n"<<endl;*/}
long long int CellT2::get_data(){return data;}
void CellT2::set_data(long long int d){data = d;}
void CellT2::printData(){
	cout<<"data: "<<data<<endl;
}
bool CellT2::isSame(CellT2 * data){ return 0; }



//////////////////////////////// CellT3 /////////////////////////////////

CellT3::CellT3(){ptrs = new Lista<VQueries>;}
CellT3::CellT3(uint64_t st, uint64_t e, uint32_t c, Op_t o, uint64_t v){
	ptrs = new Lista<VQueries>;
	start = st;
	end = e;
	value = v;
	op = o;
	col = c;
	conflicts = NULL;
	consize = 0;
}
CellT3::~CellT3(){ delete [] conflicts; ptrs->popall(); delete ptrs;}

void CellT3::set_conflicts(bool *c){conflicts = c;}
void CellT3::set_consize(uint64_t s){consize = s;}
void CellT3::set_start(uint64_t st){start = st;}
void CellT3::set_end(uint64_t e){end=e;}
void CellT3::set_value(uint64_t v){value = v;}
void CellT3::set_col(uint32_t c){col = c;};
void CellT3::set_op(Op_t ooo){op = ooo;};
Op_t CellT3::get_op(){return op;}
uint64_t CellT3::get_start(){return start;}
uint64_t CellT3::get_end(){return end;}
uint64_t CellT3::get_value(){return value;}
uint32_t CellT3::get_col(){return col;}
bool * CellT3::get_conflicts(){return conflicts;}
uint64_t CellT3::get_consize(){return consize;}
void CellT3::push_valptr(VQueries *vq){ptrs->push(vq);}
VQueries * CellT3::pop_valptr(){return ptrs->pop();}
int CellT3::cur_valptr(){ptrs->getCounter();}

Lista<VQueries> * CellT3::get_ptrs(){ptrs->preview();}

void CellT3::printData(){
    char c[3];
    switch(op){
        case Equal: strcpy(c,"="); break;
        case NotEqual: strcpy(c,"!="); break;
        case Less: strcpy(c,"<"); break;
        case LessOrEqual: strcpy(c,"<="); break;
        case Greater: strcpy(c,">"); break;
        case GreaterOrEqual: strcpy(c,">="); break;
        default: break;
    }
    cout<< start << " " << end << " | c" << col << c << value << endl;
}

bool CellT3::isSame(CellT3 * data){
	if(value != data->get_value())
		return 0;
	if(start != data->get_start())
		return 0;
	if(end != data->get_end())
		return 0;
	if(col != data->get_col())
		return 0;
	if(op != data->get_op())
		return 0;
	return 1;
}


uint64_t CellT3::createPK(){
	uint64_t key=0;
	uint64_t temp=0;
	
	temp=start%32768;
	temp=temp<<49;
	key=key+temp;
	temp=0;
	temp=end%32768;
	temp=temp<<34;
	key=key+temp;
	temp=0;
	temp=col%32768;
	temp=temp<<19;
	key=key+temp;
	temp=0;
	temp=op%16;
	temp=temp<<15;
	key=key+temp;
	temp=0;
	temp=value%32768;
	key=key+temp;

	//key=key%size;

	return key;
}

uint64_t CellT3::createPK2(){
	uint64_t key=0;
	uint64_t temp=0;
	
	temp=start%16;
	temp=temp<<16;
	key=key+temp;
	temp=0;
	temp=end%16;
	temp=temp<<12;
	key=key+temp;
	temp=0;
	temp=col%16;
	temp=temp<<8;
	key=key+temp;
	temp=0;
	temp=op%16;
	temp=temp<<4;
	key=key+temp;
	temp=0;
	temp=value%16;
	key=key+temp;

	//key=key%size;

	return key;
}




uint64_t CellT3::createPK3(){
uint64_t key=0;
uint64_t temp=0;

temp=start%65536;
temp=temp<<48;
key=key+temp;
temp=0;
temp=end%65536;
temp=temp<<32;
key=key+temp;
temp=0;
temp=col%64;
temp=temp<<27;
key=key+temp;
temp=0;
temp=op%8;
temp=temp<<24;
key=key+temp;
temp=0;
temp=value%16777216;
key=key+temp;

//key=key%size;

return key;
}