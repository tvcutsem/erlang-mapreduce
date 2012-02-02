What?
=====

A simple parallel MapReduce implementation in Erlang.
The emphasis is on simplicity and understandability of the code.

Check out the demo files for simple examples, including:
  
  *  building an inverted index of a collection of text documents
  *  word frequency count of a collection of text documents
  *  grep tool to search a collection of text documents

To compile:

	make code

To run the inverted index demo:

	cd ebin
	erl
	% in the erl prompt:
	> Index = demo_inverted_index:index(test). % index the test subdirectory
	> demo_inverted_index:query_index(Index, rover). % what files contain the word 'rover'?
    {ok,["test/dogs","test/cars"]}
    > halt().

To clean up:

	make clean

Why?
====

This implementation is part of the teaching material of my course on [multicore programming](http://soft.vub.ac.be/~tvcutsem/multicore), a course I teach at the Vrije Universiteit Brussel (VUB) in Brussels, Belgium.

The goal is to teach students both the fundamentals of MapReduce (in particular, the API of the Map and Reduce operations, and how these are combined to formulate large data processing jobs), and to increase their fluency of Erlang at the same time. The code showcases process spawning, synchronization via message passing and process termination.

Slideware
=========

I gave a 40-minute talk about this project at the [Erlang Factory Lite Brussels](https://www.erlang-factory.com/conference/Brussels). The slides are available [here](http://soft.vub.ac.be/~tvcutsem/invokedynamic/presentations/tvcutsem_MapReduce_ErlangFactory.pdf) (pdf).

In Scala
--------

I contributed a chapter discussing an adaptation of this MapReduce implementation in Scala in Philipp Haller's book on [Actors in Scala](http://www.artima.com/shop/actors_in_scala) (chapter 9, Distributed and Parallel Computing).

Acknowledgements
================

The inverted index example was taken from Joe Armstrong's [Programming Erlang](http://pragprog.com/book/jaerlang/programming-erlang) book.

Feedback
========

I welcome any feedback at `tvcutsem` at `vub.ac.be`. Or drop me a line on [twitter](https://twitter.com/#!/tvcutsem).
