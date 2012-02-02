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

% A basic parallel (but not distributed) implementation of the Google
% MapReduce pattern.

-module(mapreduce).
-export([mapreduce/3]).
-import(lists, [foreach/2]).

%% Input = [{K1, V1}]
%% Map(K1, V1, Emit) -> Emit a stream of {K2,V2} tuples
%% Reduce(K2, List[V2], Emit) -> Emit a stream of {K2,V2} tuples
%% Returns a Map[K2,List[V2]]
mapreduce(Input, Map, Reduce) ->
  S = self(),
  Pid = spawn(fun() -> master(S, Map, Reduce, Input) end),
  receive
    {Pid, Result} -> Result
  end.

spawn_workers(MasterPid, Fun, Pairs) ->
  foreach(fun({K,V}) ->
            spawn_link(fun() -> worker(MasterPid, Fun, {K,V}) end)
          end, Pairs).
  
master(Parent, Map, Reduce, Input) ->
  process_flag(trap_exit, true),
  MasterPid = self(),
  
  %% Create the mapper processes, one for each element in Input
  spawn_workers(MasterPid, Map, Input),
          
  M = length(Input),
  %% Wait for M Map processes to terminate
  Intermediate = collect_replies(M, dict:new()), 
  
  %% Create the reducer processes, one for each intermediate Key
  spawn_workers(MasterPid, Reduce, dict:to_list(Intermediate)),
  
  R = dict:size(Intermediate),
  %% Wait for R Reduce processes to terminate
  Output = collect_replies(R, dict:new()),
  Parent ! {self(), Output}.

%%	Worker must send {K2, V2} messsages to master and then terminate
worker(MasterPid, Fun, {K,V}) ->
  Fun(K, V, fun(K2,V2) -> MasterPid ! {K2, V2} end).

%% collect and merge {Key, Value} messages from N processes.
%% When N processes have terminated return a dictionary of {Key, [Value]} pairs
collect_replies(0, Dict) -> Dict;
collect_replies(N, Dict) ->
  receive
    {Key, Val} ->
      Dict1 = dict:append(Key, Val, Dict),
      collect_replies(N, Dict1);
    {'EXIT', _Who, _Why} ->
      collect_replies(N-1, Dict)
  end.