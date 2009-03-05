-module(queue_merle).

-export([start/0, set/3, getkey/2]).

start() ->
	Q0 = queue:new(),
	{ok, Pid0} = merle:connect(),
	Q1 = queue:in(Pid0,Q0),
	{ok, Pid1} = merle:connect(),
	Q2 = queue:in(Pid1,Q1),
	{ok, Pid2} = merle:connect(),
	Q3 = queue:in(Pid2,Q2),
	{ok, Pid3} = merle:connect(),
	Q4 = queue:in(Pid3,Q3),
	{ok, Pid4} = merle:connect(),
	Q5 = queue:in(Pid4,Q4),
	Q5.

set(Queue, Key, Value) ->
	Pid = queue:head(Queue),
	Status = merle:set(Pid, Key, Value),
	Queue1 = rotate(Queue, Pid),
	{Queue1, Status}.

getkey(Queue, Key) ->
	Pid = queue:head(Queue),
	Status = merle:getkey(Pid, Key),
	Queue1 = rotate(Queue, Pid),
	{Queue1, Status}.

rotate(Queue, Pid) ->
	Queue1 = queue:tail(Queue),
	Queue2 = queue:snoc(Queue1, Pid),
	Queue2.
