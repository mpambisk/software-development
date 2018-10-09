all: compile

compile:
	g++ -o ask Main.cpp Journal.cpp VQueries.cpp Cells.cpp -g3 -pthread
	g++ -o compare Compare.cpp

clean:
	rm -f ask 
	rm -f compare
	rm -f *.txt

#basic
1run1:
	./ask 1 1 < ../DATASETS/basic.bin
2run1:
	./ask 1 2 < ../DATASETS/basic.bin
3run1:
	./ask 2 2 < ../DATASETS/basic.bin
4run1:
	./ask 1 3 < ../DATASETS/basic.bin
5run1:
	./ask 3 1 < ../DATASETS/basic.bin
6run1:
	./ask 3 2 < ../DATASETS/basic.bin
7run1:
	./ask 3 3 < ../DATASETS/basic.bin

#small
1run2:
	./ask 1 1 < ../DATASETS/small.bin
2run2:
	./ask 1 2 < ../DATASETS/small.bin
3run2:
	./ask 2 2 < ../DATASETS/small.bin
4run2:
	./ask 1 3 < ../DATASETS/small.bin
5run2:
	./ask 3 1 < ../DATASETS/small.bin
6run2:
	./ask 3 2 < ../DATASETS/small.bin
7run2:
	./ask 3 3 < ../DATASETS/small.bin

#medium
1run3:
	./ask 1 1 < ../DATASETS/medium.bin
2run3:
	./ask 1 2 < ../DATASETS/medium.bin
3run3:
	./ask 2 2 < ../DATASETS/medium.bin
4run3:
	./ask 1 3 < ../DATASETS/medium.bin
5run3:
	./ask 3 1 < ../DATASETS/medium.bin
6run3:
	./ask 3 2 < ../DATASETS/medium.bin
7run3:
	./ask 3 3 < ../DATASETS/medium.bin

#comparisons
c1:
	./compare ../DATASETS/basic.out.bin
c2:
	./compare ../DATASETS/small.out.bin
c3:	
	./compare ../DATASETS/medium.out.bin
