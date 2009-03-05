%% Copyright 2009, Joe Williams <joe@joetify.com>
%% Copyright 2009, Nick Gerakines <nick@gerakines.net>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%%
%% @author Joseph Williams <joe@joetify.com>
%% @copyright 2008 Joseph Williams
%% @version 0.3
%% @seealso http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
%% @doc An Erlang memcached client.
%%
%% This code is available as Open Source Software under the MIT license.
%%
%% Updates at http://github.com/joewilliams/merle/

-module(merle).
-behaviour(gen_server2).

-author("Joe Williams <joe@joetify.com>").
-version("Version: 0.3").

-define(SERVER, ?MODULE).
-define(TIMEOUT, 5000).
-define(RANDOM_MAX, 65535).
-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 11211).
-define(TCP_OPTS, [
    binary, {packet, raw}, {nodelay, true},{reuseaddr, true}, {active, true}
]).

%% gen_server API
-export([
    stats/1, stats/2, version/1, getkey/2, delete/3, set/5, add/5, replace/3,
    replace/5, cas/6, set/3, flushall/1, flushall/2, verbosity/2, add/3,
    cas/4, getskey/2, connect/0, connect/2, delete/2, disconnect/1
]).

%% gen_server callbacks
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

%% @doc retrieve memcached stats
stats(Pid) ->
	gen_server2:call(Pid, {stats}).

%% @doc retrieve memcached stats based on args
stats(Pid, Args) when is_atom(Args)->
	stats(Pid, atom_to_list(Args));
stats(Pid, Args) ->
	gen_server2:call(Pid, {stats, {Args}}).

%% @doc retrieve memcached version
version(Pid) ->
	gen_server2:call(Pid, {version}).

%% @doc set the verbosity level of the logging output
verbosity(Pid, Args) when is_integer(Args) ->
	verbosity(Pid, integer_to_list(Args));
verbosity(Pid, Args)->
	case gen_server2:call(Pid, {verbosity, {Args}}) of
		["OK"] -> ok;
		[X] -> X
	end.

%% @doc invalidate all existing items immediately
flushall(Pid) ->
	case gen_server2:call(Pid, {flushall}) of
		["OK"] -> ok;
		[X] -> X
	end.

%% @doc invalidate all existing items based on the expire time argument
flushall(Pid, Delay) when is_integer(Delay) ->
	flushall(Pid, integer_to_list(Delay));
flushall(Pid, Delay) ->
	case gen_server2:call(Pid, {flushall, {Delay}}) of
		["OK"] -> ok;
		[X] -> X
	end.

%% @doc retrieve value based off of key
getkey(Pid, Key) when is_atom(Key) ->
	getkey(Pid, atom_to_list(Key));
getkey(Pid, Key) ->
	case gen_server2:call(Pid, {getkey,{Key}}) of
	    ["END"] -> undefined;
	    [X] -> X
	end.

%% @doc retrieve value based off of key for use with cas
getskey(Pid, Key) when is_atom(Key) ->
	getskey(Pid, atom_to_list(Key));
getskey(Pid, Key) ->
	case gen_server2:call(Pid, {getskey,{Key}}) of
	    ["END"] -> undefined;
	    [X] -> X
	end.

%% @doc delete a key
delete(Pid, Key) ->
	delete(Pid, Key, "0").

delete(Pid, Key, Time) when is_atom(Key) ->
	delete(Pid, atom_to_list(Key), Time);
delete(Pid, Key, Time) when is_integer(Time) ->
	delete(Pid, Key, integer_to_list(Time));
delete(Pid, Key, Time) ->
	case gen_server2:call(Pid, {delete, {Key, Time}}) of
		["DELETED"] -> ok;
		["NOT_FOUND"] -> not_found;
		[X] -> X
	end.

%% Time is the amount of time in seconds
%% the client wishes the server to refuse
%% "add" and "replace" commands with this key.

%%
%% Storage Commands
%%

%% *Flag* is an arbitrary 16-bit unsigned integer (written out in
%% decimal) that the server stores along with the Value and sends back
%% when the item is retrieved.
%%
%% *ExpTime* is expiration time. If it's 0, the item never expires
%% (although it may be deleted from the cache to make place for other
%%  items).
%%
%% *CasUniq* is a unique 64-bit value of an existing entry.
%% Clients should use the value returned from the "gets" command
%% when issuing "cas" updates.
%%
%% *Value* is the value you want to store.

%% @doc Store a key/value pair.
set(Pid, Key, Value) ->
    Flag = random:uniform(?RANDOM_MAX),
    set(Pid, Key, integer_to_list(Flag), "0", Value).

set(Pid, Key, Flag, ExpTime, Value) when is_atom(Key) ->
	set(Pid, atom_to_list(Key), Flag, ExpTime, Value);
set(Pid, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    set(Pid, Key, integer_to_list(Flag), ExpTime, Value);
set(Pid, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    set(Pid, Key, Flag, integer_to_list(ExpTime), Value);
set(Pid, Key, Flag, ExpTime, Value) ->
	case gen_server2:call(Pid, {set, {Key, Flag, ExpTime, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc Store a key/value pair if it doesn't already exist.
add(Pid, Key, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	add(Pid, Key, integer_to_list(Flag), "0", Value).

add(Pid, Key, Flag, ExpTime, Value) when is_atom(Key) ->
	add(Pid, atom_to_list(Key), Flag, ExpTime, Value);
add(Pid, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    add(Pid, Key, integer_to_list(Flag), ExpTime, Value);
add(Pid, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    add(Pid, Key, Flag, integer_to_list(ExpTime), Value);
add(Pid, Key, Flag, ExpTime, Value) ->
	case gen_server2:call(Pid, {add, {Key, Flag, ExpTime, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc Replace an existing key/value pair.
replace(Pid, Key, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	replace(Pid, Key, integer_to_list(Flag), "0", Value).

replace(Pid, Key, Flag, ExpTime, Value) when is_atom(Key) ->
	replace(Pid, atom_to_list(Key), Flag, ExpTime, Value);
replace(Pid, Key, Flag, ExpTime, Value) when is_integer(Flag) ->
    replace(Pid, Key, integer_to_list(Flag), ExpTime, Value);
replace(Pid, Key, Flag, ExpTime, Value) when is_integer(ExpTime) ->
    replace(Pid, Key, Flag, integer_to_list(ExpTime), Value);
replace(Pid, Key, Flag, ExpTime, Value) ->
	case gen_server2:call(Pid, {replace, {Key, Flag, ExpTime, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc Store a key/value pair if possible.
cas(Pid, Key, CasUniq, Value) ->
	Flag = random:uniform(?RANDOM_MAX),
	cas(Pid, Key, integer_to_list(Flag), "0", CasUniq, Value).

cas(Pid, Key, Flag, ExpTime, CasUniq, Value) when is_atom(Key) ->
	cas(Pid, atom_to_list(Key), Flag, ExpTime, CasUniq, Value);
cas(Pid, Key, Flag, ExpTime, CasUniq, Value) when is_integer(Flag) ->
    cas(Pid, Key, integer_to_list(Flag), ExpTime, CasUniq, Value);
cas(Pid, Key, Flag, ExpTime, CasUniq, Value) when is_integer(ExpTime) ->
    cas(Pid, Key, Flag, integer_to_list(ExpTime), CasUniq, Value);
cas(Pid, Key, Flag, ExpTime, CasUniq, Value) when is_integer(CasUniq) ->
    cas(Pid, Key, Flag, ExpTime, integer_to_list(CasUniq), Value);
cas(Pid, Key, Flag, ExpTime, CasUniq, Value) ->
	case gen_server2:call(Pid, {cas, {Key, Flag, ExpTime, CasUniq, Value}}) of
	    ["STORED"] -> ok;
	    ["NOT_STORED"] -> not_stored;
	    [X] -> X
	end.

%% @doc connect to memcached with defaults
connect() ->
	connect(?DEFAULT_HOST, ?DEFAULT_PORT).

%% @doc connect to memcached
connect(Host, Port) ->
	start_link(Host, Port).

%% @doc disconnect from memcached
disconnect(Pid) ->
	gen_server2:call(Pid, {stop}),
	ok.

%% @private
start_link(Host, Port) ->
    gen_server2:start_link(?MODULE, [Host, Port], []).

%% @private
init([Host, Port]) ->
    gen_tcp:connect(Host, Port, ?TCP_OPTS).

handle_call({stop}, _From, Socket) ->
    {stop, requested_disconnect, Socket};

handle_call({stats}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"stats">>])),
    {reply, Reply, Socket};

handle_call({stats, {Args}}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"stats ">>, Args])),
    {reply, Reply, Socket};

handle_call({version}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"version">>])),
    {reply, Reply, Socket};

handle_call({verbosity, {Args}}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"verbosity ">>, Args])),
    {reply, Reply, Socket};

handle_call({flushall}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"flush_all">>])),
    {reply, Reply, Socket};

handle_call({flushall, {Delay}}, _From, Socket) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"flush_all ">>, Delay])),
    {reply, Reply, Socket};

handle_call({getkey, {Key}}, _From, Socket) ->
    Reply = send_get_cmd(Socket, iolist_to_binary([<<"get ">>, Key])),
    {reply, Reply, Socket};

handle_call({getskey, {Key}}, _From, Socket) ->
    Reply = send_gets_cmd(Socket, iolist_to_binary([<<"gets ">>, Key])),
    {reply, [Reply], Socket};

handle_call({delete, {Key, Time}}, _From, Socket) ->
    Reply = send_generic_cmd(
        Socket,
        iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time])
    ),
    {reply, Reply, Socket};

handle_call({set, {Key, Flag, ExpTime, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"set ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes
        ]),
        Bin
    ),
    {reply, Reply, Socket};

handle_call({add, {Key, Flag, ExpTime, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"add ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes
        ]),
        Bin
    ),
    {reply, Reply, Socket};

handle_call({replace, {Key, Flag, ExpTime, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"replace ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>,
            Bytes
        ]),
    	Bin
    ),
    {reply, Reply, Socket};

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value}}, _From, Socket) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(
        Socket,
        iolist_to_binary([
            <<"cas ">>, Key, <<" ">>, Flag, <<" ">>, ExpTime, <<" ">>, Bytes,
            <<" ">>, CasUniq
        ]),
        Bin
    ),
    {reply, Reply, Socket}.

%% @private
handle_cast(_Msg, State) -> {noreply, State}.

%% @private
handle_info(_Info, State) -> {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
%% @doc Closes the socket
terminate(_Reason, Socket) ->
    gen_tcp:close(Socket),
    ok.

%% @private
%% @doc send_generic_cmd/2 function for simple informational and deletion commands
send_generic_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_simple_reply(),
	Reply.

%% @private
%% @doc send_storage_cmd/3 funtion for storage commands
send_storage_cmd(Socket, Cmd, Value) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    gen_tcp:send(Socket, <<Value/binary, "\r\n">>),
    Reply = recv_simple_reply(),
   	Reply.

%% @private
%% @doc send_get_cmd/2 function for retreival commands
send_get_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_complex_get_reply(Socket),
	Reply.

%% @private
%% @doc send_gets_cmd/2 function for cas retreival commands
send_gets_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_complex_gets_reply(Socket),
	Reply.

%% @private
%% @doc receive function for simple responses (not containing VALUEs)
recv_simple_reply() ->
	receive
	  	{tcp,_,Data} ->
        	string:tokens(binary_to_list(Data), "\r\n");
        {error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc receive function for respones containing VALUEs
recv_complex_get_reply(Socket) ->
	receive
		%% For receiving get responses where the key does not exist
		{tcp, Socket, <<"END\r\n">>} -> ["END"];
		%% For receiving get responses containing data
		{tcp, Socket, Data} ->
			%% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
  			Parse = io_lib:fread("~s ~s ~u ~u\r\n", binary_to_list(Data)),
  			{ok,[_,_,_,Bytes], ListBin} = Parse,
  			Bin = list_to_binary(ListBin),
  			Reply = get_data(Socket, Bin, Bytes, length(ListBin)),
  			[Reply];
  		{error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc receive function for cas responses containing VALUEs
recv_complex_gets_reply(Socket) ->
	receive
		%% For receiving get responses where the key does not exist
		{tcp, Socket, <<"END\r\n">>} -> ["END"];
		%% For receiving get responses containing data
		{tcp, Socket, Data} ->
			%% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
  			Parse = io_lib:fread("~s ~s ~u ~u ~u\r\n", binary_to_list(Data)),
  			{ok,[_,_,_,Bytes,CasUniq], ListBin} = Parse,
  			Bin = list_to_binary(ListBin),
  			Reply = get_data(Socket, Bin, Bytes, length(ListBin)),
  			[CasUniq, Reply];
  		{error, closed} ->
  			connection_closed
    after ?TIMEOUT -> timeout
    end.

%% @private
%% @doc recieve loop to get all data
get_data(Socket, Bin, Bytes, Len) when Len < Bytes + 7->
    receive
        {tcp, Socket, Data} ->
            Combined = <<Bin/binary, Data/binary>>,
            get_data(Socket, Combined, Bytes, size(Combined));
     	{error, closed} ->
  			connection_closed
        after ?TIMEOUT -> timeout
    end;
get_data(_, Data, Bytes, _) ->
	<<Bin:Bytes/binary, "\r\nEND\r\n">> = Data,
    binary_to_term(Bin).
