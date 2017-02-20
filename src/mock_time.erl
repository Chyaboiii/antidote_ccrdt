-module(mock_time).

-behaviour(gen_server).

%% api

-export([start_link/0,
         system_time/1,
         get_time/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% api

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

system_time(_) ->
    gen_server:call(?MODULE, timestamp).

get_time() ->
    gen_server:call(?MODULE, get_time).

init([]) ->
    {ok, 0}.

%% gen_server callbacks

handle_call(timestamp, _From, State) ->
    {reply, State + 1, State + 1};
handle_call(get_time, _From, State) ->
    {reply, State, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.