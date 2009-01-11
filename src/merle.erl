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
%% @doc A memcached client.
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
-export([start_link/2, stats/0, get/1,
	delete/2,set/4,add/4,replace/4,append/2,prepend/2,
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

handle_call({stats}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, iolist_to_binary([<<"stats">>])),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({getkey, {Key}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, iolist_to_binary([<<"get ">>, Key])),
    {reply, Reply, S#state{socket = Socket}};

handle_call({delete, {Key, Time}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, iolist_to_binary([<<"delete ">>, Key, <<" ">>, Time])),
    {reply, Reply, S#state{socket = Socket}};

handle_call({set, {Key, Flag, ExpTime, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_cmd(Socket, iolist_to_binary([<<"set ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({add, {Key, Flag, ExpTime, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_cmd(Socket, iolist_to_binary([<<"add ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};

handle_call({replace, {Key, Flag, ExpTime, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_cmd(Socket, iolist_to_binary([<<"replace ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({append, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_cmd(Socket, iolist_to_binary([<<"append ">>, Key, <<" 0 0 ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};

handle_call({prepend, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_cmd(Socket, iolist_to_binary([<<"prepend ">>, Key, <<" 0 0 ">>, Bytes]), Bin),
    {reply, Reply, S#state{socket = Socket}};

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
	Bytes = integer_to_list(size(Bin)),
    Reply = send_cmd(Socket, iolist_to_binary([<<"cas ">>, Key, <<" ">>, Flag, <<" ">>, 
    	ExpTime, <<" ">>, Bytes, <<" ">>, CasUniq]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({increment, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
    Reply = send_cmd(Socket, iolist_to_binary([<<"incr ">>, Key]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({decrement, {Key, Value}}, _From, #state{socket = Socket} = S) ->
	Bin = list_to_binary(Value),
    Reply = send_cmd(Socket, iolist_to_binary([<<"decr ">>, Key]), Bin),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({quit}, _From, #state{socket = Socket} = _) ->
	gen_tcp:close(Socket),
	{reply, ok, {}}.

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
%% Retrieval Commands
%%	

%% Retrieve memcached stats
stats() ->
	gen_server:call(?SERVER, {stats}).

%% Retrieve value based off of key
get(Key) ->
	gen_server:call(?SERVER, {getkey,{Key}}).

%%
%% Deletion Commands
%%	

%% Delete a key and specify time. 
%%
%% Time is the amount of time in seconds
%% the client wishes the server to refuse 
%% "add" and "replace" commands with this key.
delete(Key, Time) ->
	gen_server:call(?SERVER, {delete, {Key, Time}}).

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

%% set - "store this value"
set(Key, Flag, ExpTime, Value) ->
	gen_server:call(?SERVER, {set, {Key, Flag, ExpTime, Value}}).

%% add - "store this value, but only if the server *doesn't* already hold Value for this key"
add(Key, Flag, ExpTime, Value) ->
	gen_server:call(?SERVER, {add, {Key, Flag, ExpTime, Value}}).

%% replace - "store this value, but only if the server *does* already hold Value for this key"
replace(Key, Flag, ExpTime, Value) ->
	gen_server:call(?SERVER, {replace, {Key, Flag, ExpTime, Value}}).

%% append - "add this value to an existing key after existing Value"
append(Key, Value) ->
	gen_server:call(?SERVER, {append, {Key, Value}}).	
	
%% prepend - "add this value to an existing key before existing Value"
prepend(Key, Value) ->
	gen_server:call(?SERVER, {prepend, {Key, Value}}).
	
%% cas - "store this Vvlue but only if no one else has updated since I last fetched it"
cas(Key, Flag, ExpTime, CasUniq, Value) ->
	gen_server:call(?SERVER, {cas, {Key, Flag, ExpTime, CasUniq, Value}}).

%%	
%% Increment/Decrement Commands
%%
%% Commands "incr" and "decr" are used to change Value for some item
%% in-place, incrementing or decrementing it.

%% increment
increment(Key, Value) ->
	gen_server:call(?SERVER, {increment, {Key, Value}}).

%% decrement
decrement(Key, Value) ->
	gen_server:call(?SERVER, {decrement, {Key, Value}}).

%%
%% Exit
%%	

%% Close the socket
quit() ->
	gen_server:call(?SERVER, {quit}).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% send_cmd function for simple retrieval and deletion commands
send_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    Reply = recv_reply(),
    Reply.

%% send_cmd funtion for storage commands
send_cmd(Socket, Cmd, Value) ->
    gen_tcp:send(Socket, <<Cmd/binary, "\r\n">>),
    gen_tcp:send(Socket, <<Value/binary, "\r\n">>),
    Reply = recv_reply(),
    Reply.

recv_reply() ->
    receive
	{tcp,_,Reply} ->
	    Reply
    after 5000 ->
	    timeout
    end.
