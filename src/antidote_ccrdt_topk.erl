%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%
%% antidote_ccrdt_topk:
%% A computational CRDT that computes a top-K.

-module(antidote_ccrdt_topk).
-behaviour(antidote_ccrdt).
-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
    new/1,
    new/2,
    value/1,
    downstream/2,
    update/2,
    equal/2,
    to_binary/1,
    from_binary/1,
    is_operation/1,
    is_replicate_tagged/1,
    can_compact/2,
    compact_ops/2,
    require_state_downstream/1
]).

-type playerid() :: binary().
-type score() :: integer().
-type pair() :: {playerid(), score()} | {nil, nil}.

-type observable() :: #{playerid() => score()}.

-type topk() :: {
    observable(),
    pos_integer()
}.

-type prepare_update() :: {add, pair()}.
-type effect_update() :: {add, pair()}.

%% Creates a new `topk()` with a size of 100.
-spec new() -> topk().
new() ->
    new(1). %% TOOD: alterar

%% Creates a new `topk()` with the given `Size`.
-spec new(pos_integer()) -> topk().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, Size}.

%% Creates a new `topk()` with the given `Topk` and `Size`.
-spec new(observable(), pos_integer()) -> topk().
new(Topk, Size) when is_integer(Size), Size > 0 ->
    {Topk, Size};
new(_, _) ->
    new().

%% Returns the value of the `topk()`.
-spec value(topk()) -> list().
value({Top, _}) ->
    lists:sort(fun({N1, V1}, {N2, V2}) -> V1 > V2 orelse (V1 == V2 andalso N1 > N2) end, maps:to_list(Top)).

%% Generates an `effect_update()` from a `prepare_update()`.
%%
%% The supported `prepare_update()` for this data type are:
%% - `{add, pair()}`
-spec downstream(prepare_update(), topk()) -> {ok, effect_update() | noop}.
downstream({add, Elem}, Top) ->
    case changes_state(Elem, Top) of
        true -> {ok, {add, Elem}};
        false -> {ok, noop}
    end.

%% Executes an `effect_update()` operation and returns the resulting state.
%%
%% The executable `effect_update()` for this data type are:
%% - `{add, pair()}`
-spec update(effect_update(), topk()) -> {ok, topk()}.
update({add, {Id, Score}}, TopK) when is_binary(Id), is_integer(Score) ->
    {ok, add(Id, Score, TopK)};
update({add_map, Map}, TopK) ->
    {ok, add_map(Map, TopK)}.

%% Compares the two given `topk()` states.
-spec equal(topk(), topk()) -> boolean().
equal({Top1, Size1}, {Top2, Size2}) ->
    Top1 =:= Top2 andalso Size1 =:= Size2.

%% Converts the given `topk()` state into an Erlang `binary()`.
-spec to_binary(topk()) -> binary().
to_binary(TopK) ->
    term_to_binary(TopK).

%% Converts a given Erlang `binary()` into a `topk()`.
-spec from_binary(binary()) -> {ok, topk()}.
from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% Checks if the given `prepare_update()` is supported by the `topk()`.
-spec is_operation(any()) -> boolean().
is_operation({add, {Id, Score}}) when is_binary(Id), is_integer(Score) -> true;
is_operation(_) -> false.

%% Checks if the given `effect_update()` is tagged for replication.
-spec is_replicate_tagged(effect_update()) -> boolean().
is_replicate_tagged(_) -> false.

%% Checks if the given `effect_update()` operations can be compacted.
-spec can_compact(effect_update(), effect_update()) -> boolean().
can_compact(_, _) -> true.

%% Compacts the given `effect_update()` operations.
-spec compact_ops(effect_update(), effect_update()) -> {effect_update(), effect_update()}.
compact_ops({add, {Id1, Score1}}, {add, {Id2, Score2}}) ->
    NewOp = {add_map, #{Id1 => Score1, Id2 => Score2}},
    {noop, NewOp};
compact_ops({add, {Id, Score}}, {add_map, Map}) ->
    NewOp = {add_map, maps:put(Id, Score, Map)},
    {noop, NewOp};
compact_ops(Op1 = {add_map, _}, Op2 = {add, _}) ->
    compact_ops(Op2, Op1);
compact_ops({add_map, Map1}, {add_map, Map2}) ->
    NewOp = {add_map, maps:merge(Map1, Map2)},
    {noop, NewOp}.

%% Checks if the data type needs to know its current state to generate
%% `update_effect()` operations.
-spec require_state_downstream(any()) -> boolean().
require_state_downstream(_) -> true.

%%%% Private

%% Attempts to add the `playerid()`, `score()` pair to the `topk()`.
-spec add(playerid(), score(), topk()) -> topk().
add(Id, Score, {Top, Size}) ->
    {maps:put(Id, Score, Top), Size}.

add_map(Map, {TopK, Size}) ->
    {maps:merge(TopK, Map), Size}.

%% Checks if attempting to add the given `pair()` to the `topk()` will alter its state.
-spec changes_state(pair(), topk()) -> boolean().
changes_state({_, Score}, {_, Size}) ->
    Score > Size.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% Tests the `new/0` function.
new_test() ->
    ?assertEqual({#{}, 100}, new()).

%% Tests the `value/1` function.
value_test() ->
    Top = {#{<<"foo">> => 102, <<"bar">> => 101}, 100},
    ?assertEqual([{<<"foo">>, 102}, {<<"bar">>, 101}], value(Top)).

%% Tests several `prepare_update()` operations.
downstream_add_test() ->
    Top = {#{<<"foo">> => 102, <<"bar">> => 101}, 100},
    {ok, noop} = downstream({add, {<<"baz">>, 1}}, Top),
    {ok, {add, {<<"baz">>, 500}}} = downstream({add, {<<"baz">>, 500}}, Top).

%% Tests several `effect_update()` operations.
update_add_test() ->
    Top0 = new(100),
    {ok, Top1} = update({add, {<<"foo">>, 101}}, Top0),
    {ok, Top2} = update({add, {<<"bar">>, 102}}, Top1),
    ?assertEqual([{<<"bar">>, 102}, {<<"foo">>, 101}], value(Top2)).

compaction_test() ->
    Expected = {noop, {add_map, #{<<"bar">> => 200, <<"foo">> => 150}}},
    Result1 = compact_ops({add, {<<"foo">>, 150}}, {add, {<<"bar">>, 200}}),
    ?assertEqual(Result1, Expected),
    Result2 = compact_ops({add, {<<"foo">>, 150}}, {add_map, #{<<"bar">> => 200}}),
    ?assertEqual(Result2, Expected),
    Result3 = compact_ops({add_map, #{<<"bar">> => 200}}, {add, {<<"foo">>, 150}}),
    ?assertEqual(Result3, Expected),
    Result4 = compact_ops({add_map, #{<<"foo">> => 150}}, {add_map, #{<<"bar">> => 200}}),
    ?assertEqual(Result4, Expected).
            
-endif.


