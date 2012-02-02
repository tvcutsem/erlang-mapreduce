SOURCES=mapreduce.erl mapreduce_sequential.erl demo_frequency.erl demo_grep.erl demo_inverted_index.erl
OBJECTS=$(SOURCES:.erl=.beam)

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc -o ebin $<

code: $(SOURCES) $(OBJECTS)
	cp -R test ebin

clean:
	rm -rf ebin
