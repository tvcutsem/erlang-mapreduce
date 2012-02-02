% Copyright (c) 2011-2012, Tom Van Cutsem, Vrije Universiteit Brussel
% All rights reserved.
%
% Redistribution and use in source and binary forms, with or without
% modification, are permitted provided that the following conditions are met:
%    * Redistributions of source code must retain the above copyright
%      notice, this list of conditions and the following disclaimer.
%    * Redistributions in binary form must reproduce the above copyright
%      notice, this list of conditions and the following disclaimer in the
%      documentation and/or other materials provided with the distribution.
%    * Neither the name of the Vrije Universiteit Brussel nor the
%      names of its contributors may be used to endorse or promote products
%      derived from this software without specific prior written permission.
%
%THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
%ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
%WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%DISCLAIMED. IN NO EVENT SHALL VRIJE UNIVERSITEIT BRUSSEL BE LIABLE FOR ANY
%DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
%(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES
%LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
%ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

% Example of building an inverted index of a set of text documents

-module(demo_inverted_index).
-export([index/1, query_index/2, list_numbered_files/1]).
-import(mapreduce, [mapreduce/3]).

%% Auxiliary function to generate {Index, FileName} input
list_numbered_files(DirName) ->
  {ok, Files} = file:list_dir(DirName),
  FullFiles = [ filename:join(DirName, File) || File <- Files ],
  Indices = lists:seq(1, length(Files)),
  lists:zip(Indices, FullFiles). % {Index, FileName} tuples
  
%% Inverse Index
index(DirName) ->
  NumberedFiles = list_numbered_files(DirName),
  mapreduce(NumberedFiles, fun find_words/3, fun remove_duplicates/3).

% this function is used as a Map function
find_words(_Index, FileName, Emit) ->
  {ok, [Words]} = file:consult(FileName),
  lists:foreach(fun (Word) -> Emit(Word, FileName) end, Words).

% this function is used as a Reduce function
remove_duplicates(Word, FileNames, Emit) ->
  UniqueFiles = sets:to_list(sets:from_list(FileNames)),
  lists:foreach(fun (FileName) -> Emit(Word, FileName) end, UniqueFiles).

query_index(Index, Word) ->
  dict:find(Word, Index).