%% Copyright (c) 2009 Joseph Williams <joe@joetify.com>
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
%% @version pre 0.1
%% @doc an erlang memcached client.
%%
%% This code is available as Open Source Software under the MIT license.
%%
%% Updates at http://github.com/joewilliams/merle/

-module(merle).

-author("Joseph Williams <joe@joetify.com>").
-version("Version: pre 0.1").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TCP_OPTS, [binary, {packet, raw}, {nodelay, true}, 
	{reuseaddr, true}, {active, true}]).

%% API
-export([start_link/2, stats/0, stats_args/1, version/0,
	get/1,delete/2,set/4,add/4,replace/4,append/2,prepend/2,
	increment/2,decrement/2,cas/5,quit/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	terminate/2, code_change/3]).

-record(state, {socket}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Host, Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Host, Port], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Host, Port]) ->
	{ok, Socket} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
  	{ok, #state{socket = Socket}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

%%
%% Infromational Commands
%%	

handle_call({stats}, _From, #state{socket = Socket} = S) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"stats">>])),
    {reply, Reply, S#state{socket = Socket}};

handle_call({stats, {Args}}, _From, #state{socket = Socket} = S) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"stats ">>, Args])),
    {reply, Reply, S#state{socket = Socket}};

handle_call({version}, _From, #state{socket = Socket} = S) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"version">>])),
    {reply, Reply, S#state{socket = Socket}};

%%    
%% Retrieval Command
%%
    
handle_call({getkey, {Key}}, _From, #state{socket = Socket} = S) ->
    Reply = send_get_cmd(Socket, iolist_to_binary([<<"get ">>, Key])),
    {reply, Reply, S#state{socket = Socket}};

%%
%% Deletion Command
%%

handle_call({delete, {Key, Time}}, _From, #state{socket = Socket} = S) ->
    Reply = send_generic_cmd(Socket, iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time])),
    {reply, Reply, S#state{socket = Socket}};

%%	
%% Storage Commands
%%

handle_call({set, {Key, Flag, ExpTime, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"set ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({add, {Key, Flag, ExpTime, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"add ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};

handle_call({replace, {Key, Flag, ExpTime, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"replace ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({append, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"append ">>, Key, <<" 0 0 ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};

handle_call({prepend, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"prepend ">>, Key, <<" 0 0 ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"cas ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes, <<" ">>, CasUniq]), Bin),
    {reply, Reply, S#state{socket = Socket}};

%%	
%% Increment/Decrement Commands
%%
    
handle_call({increment, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"incr ">>, Key]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({decrement, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = term_to_binary(Value),
    Reply = send_storage_cmd(Socket, iolist_to_binary([<<"decr ">>, Key]), Bin),
    {reply, Reply, S#state{socket = Socket}};

%%
%% Exit
%%
    
handle_call({quit}, _From, #state{socket = Socket} = _) ->
	gen_tcp:close(Socket),
	{reply, quit, {}}.
	

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% API functions
%%--------------------------------------------------------------------

%% Command descriptions savagely ripped from:
%% http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt

%%
%% Infromational Commands
%%	

%% @doc Retrieve memcached stats
stats() ->
	gen_server:call(?SERVER, {stats}).

%% @doc Retrieve memcached stats based on args
stats_args(Args) ->
	gen_server:call(?SERVER, {stats,{Args}}).

%% @doc Retrieve memcached version
version() ->
	gen_server:call(?SERVER, {version}).

%%    
%% Retrieval Command
%%

%% @doc Retrieve value based off of key
get(Key) ->
	gen_server:call(?SERVER, {getkey,{Key}}).

%%
%% Deletion Command
%%	

%% @doc Delete a key and specify time. 
delete(Key, Time) ->
	gen_server:call(?SERVER, {delete, {Key, Time}}).

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

%% @doc set - "store this value"
set(Key, Flag, ExpTime, Value) ->
	gen_server:call(?SERVER, {set, {Key, Flag, ExpTime, Value}}).

%% @doc add - "store this value, but only if the server *doesn't* already hold Value for this key"
add(Key, Flag, ExpTime, Value) ->
	gen_server:call(?SERVER, {add, {Key, Flag, ExpTime, Value}}).

%% @doc replace - "store this value, but only if the server *does* already hold Value for this key"
replace(Key, Flag, ExpTime, Value) ->
	gen_server:call(?SERVER, {replace, {Key, Flag, ExpTime, Value}}).

%% @doc append - "add this value to an existing key after existing Value"
append(Key, Value) ->
	gen_server:call(?SERVER, {append, {Key, Value}}).	
	
%% @doc prepend - "add this value to an existing key before existing Value"
prepend(Key, Value) ->
	gen_server:call(?SERVER, {prepend, {Key, Value}}).
	
%% @doc cas - "store this Vvlue but only if no one else has updated since I last fetched it"
cas(Key, Flag, ExpTime, CasUniq, Value) ->
	gen_server:call(?SERVER, {cas, {Key, Flag, ExpTime, CasUniq, Value}}).

%%	
%% Increment/Decrement Commands
%%

%% Commands "incr" and "decr" are used to change Value for some item
%% in-place, incrementing or decrementing it.

%% @doc increment the value
increment(Key, Value) ->
	gen_server:call(?SERVER, {increment, {Key, Value}}).

%% @doc decrement the value
decrement(Key, Value) ->
	gen_server:call(?SERVER, {decrement, {Key, Value}}).

%%
%% Exit
%%	

%% @doc close the socket
quit() ->
	gen_server:call(?SERVER, {quit}).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @private
%% @doc send_generic_cmd function for simple informational and deletion commands
send_generic_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_simple_reply(),
	Reply.
	
%% @private
%% @doc send_storage_cmd funtion for storage commands
send_storage_cmd(Socket, Cmd, Value) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    gen_tcp:send(Socket, <<Value/binary, "\r\n">>),
    Reply = recv_simple_reply(),
   	Reply.

%% @private
%% @doc send_get_cmd function for retreival commands
send_get_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
	Reply = recv_complex_reply(Socket),
	Reply.

%% @private
%% @doc receive function for simple responses (not containing VALUEs)
recv_simple_reply() ->
	receive
	  	{tcp,_,Data} ->
        	string:tokens(binary_to_list(Data), "\r\n")
    after 5000 ->
   		timeout
    end.

%% @private
%% @doc receive function for reponses containing a VALUE
recv_complex_reply(Socket) ->
	receive
		{tcp, Socket, Data} ->
			%% Reply format <<"VALUE SOMEKEY FLAG BYTES\r\nSOMEVALUE\r\nEND\r\n">>
  			Parse = io_lib:fread("~s ~s ~u ~u\r\n", binary_to_list(Data)),
  			{ok,[_,_,_,Bytes], Rest} = Parse,
  			RestBin = list_to_binary(Rest),
  			<<BinTerm:Bytes/binary, "\r\nEND\r\n">> = RestBin,
  			Term = binary_to_term(BinTerm),
  			[Term]
    after 5000 ->
   		timeout
    end.
