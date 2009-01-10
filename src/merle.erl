%%%%-------------------------------------------------------------------
%%% File    : merle.erl
%%% Author  : Joe Williams (joe at joetify dot com)
%%% Description : merle an Erlang memcached client
%%% License	: Released under the MIT License.
%%%
%%% Created : 20090109
%%%-------------------------------------------------------------------

-module(merle).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(TCP_OPTS, [binary, {packet, raw}, {nodelay, true}, 
	{reuseaddr, true}, {active, true}]).

%% API
-export([start_link/2, memcache_stats/0, memcache_get/1,
	memcache_delete/2,memcache_set/4,memcache_add/4,
	memcache_replace/4,memcache_append/2,memcache_prepend/2,
	memcache_cas/5,memcache_quit/0]).

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
    Reply = send_cmd(Socket, "stats"),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({getkey, {Key}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "get " ++ Key),
    {reply, Reply, S#state{socket = Socket}};

handle_call({delete, {Key, Time}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "delete " ++ Key ++ " " ++ Time),
    {reply, Reply, S#state{socket = Socket}};

handle_call({set, {Key, Flag, ExpTime, Data}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "set " ++ Key ++ " " ++ Flag ++ " " ++ ExpTime ++ " " ++ integer_to_list(length(Data)) ++ "\r\n" ++ Data),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({add, {Key, Flag, ExpTime, Data}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "add " ++ Key ++ " " ++ Flag ++ " " ++ ExpTime ++ " " ++ integer_to_list(length(Data)) ++ "\r\n" ++ Data),
    {reply, Reply, S#state{socket = Socket}};

handle_call({replace, {Key, Flag, ExpTime, Data}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "replace " ++ Key ++ " " ++ Flag ++ " " ++ ExpTime ++ " " ++ integer_to_list(length(Data)) ++ "\r\n" ++ Data),
    {reply, Reply, S#state{socket = Socket}};
    
handle_call({append, {Key, Data}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "append " ++ Key ++ " " ++ "0" ++ " " ++ "0" ++ " " ++ integer_to_list(length(Data)) ++ "\r\n" ++ Data),
    {reply, Reply, S#state{socket = Socket}};

handle_call({prepend, {Key, Data}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "prepend " ++ Key ++ " " ++ "0" ++ " " ++ "0" ++ " " ++ integer_to_list(length(Data)) ++ "\r\n" ++ Data),
    {reply, Reply, S#state{socket = Socket}};

handle_call({cas, {Key, Flag, ExpTime, CasUniq, Data}}, _From, #state{socket = Socket} = S) ->
    Reply = send_cmd(Socket, "set " ++ Key ++ " " ++ Flag ++ " " ++ ExpTime ++ " " ++ integer_to_list(length(Data)) ++ CasUniq ++ "\r\n" ++ Data),
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
memcache_stats() ->
	gen_server:call(?SERVER, {stats}).

%% Retrieve value based off of key
memcache_get(Key) ->
	gen_server:call(?SERVER, {getkey,{Key}}).

%%
%% Deletion Commands
%%	

%% Delete a key and specify time. 
%%
%% Time is the amount of time in seconds
%% the client wishes the server to refuse 
%% "add" and "replace" commands with this key.
memcache_delete(Key, Time) ->
	gen_server:call(?SERVER, {delete, {Key, Time}}).

%%	
%% Storage Commands
%%

%% *Flag* is an arbitrary 16-bit unsigned integer (written out in
%% decimal) that the server stores along with the data and sends back
%% when the item is retrieved.

%% *ExpTime* is expiration time. If it's 0, the item never expires
%% (although it may be deleted from the cache to make place for other
%%  items).

%% *CasUniq* is a unique 64-bit value of an existing entry.
%% Clients should use the value returned from the "gets" command
%% when issuing "cas" updates.

%% *Data* is the data you want to store. 

%% set - "store this data"
memcache_set(Key, Flag, ExpTime, Data) ->
	gen_server:call(?SERVER, {set, {Key, Flag, ExpTime, Data}}).

%% add - "store this data, but only if the server *doesn't* already hold data for this key"
memcache_add(Key, Flag, ExpTime, Data) ->
	gen_server:call(?SERVER, {add, {Key, Flag, ExpTime, Data}}).

%% replace - "store this data, but only if the server *does* already hold data for this key"
memcache_replace(Key, Flag, ExpTime, Data) ->
	gen_server:call(?SERVER, {replace, {Key, Flag, ExpTime, Data}}).

%% append - "add this data to an existing key after existing data"
memcache_append(Key, Data) ->
	gen_server:call(?SERVER, {append, {Key, Data}}).	
	
%% prepend - "add this data to an existing key before existing data"
memcache_prepend(Key, Data) ->
	gen_server:call(?SERVER, {prepend, {Key, Data}}).
	
%% cas - "store this data but only if no one else has updated since I last fetched it"
memcache_cas(Key, Flag, ExpTime, CasUniq, Data) ->
	gen_server:call(?SERVER, {cas, {Key, Flag, ExpTime, CasUniq, Data}}).

%% Close the socket
memcache_quit() ->
	gen_server:call(?SERVER, {quit}).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

send_cmd(Socket, Cmd) ->
    gen_tcp:send(Socket, Cmd ++ "\r\n"),
    Reply = recv_reply(),
    Reply.

recv_reply() ->
    receive
	{tcp,_,Reply} ->
	    Reply
    after 5000 ->
	    timeout
    end.


