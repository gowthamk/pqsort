all: generate pqs

generate: generate.c
	gcc -o generate generate.c

prefix.o: prefix.c
	gcc -std=gnu99 -g -c -lm -lpthread prefix.c

pqs: driver.c prefix.o
	gcc -std=gnu99 -g -o pqs driver.c prefix.o -lm -lpthread

clean:
	rm generate prefix.o pqs
