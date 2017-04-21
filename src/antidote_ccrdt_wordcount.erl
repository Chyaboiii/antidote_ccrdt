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

-module(antidote_ccrdt_wordcount).
-behaviour(antidote_ccrdt).
-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
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

new() ->
    #{}.

value(Wordcount) ->
    Wordcount.

downstream({add, File}, _) ->
    {ok, {add, File}}.

update({add, File}, Wordcount) ->
    {ok, add(Wordcount, File)}.

equal(A, B) ->
    A =:= B.

to_binary(Average) ->
    term_to_binary(Average).

from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

is_operation({add, File}) when is_binary(File) -> true;
is_operation(_) -> false.

is_replicate_tagged(_) -> false.

can_compact(_, _) -> true.

compact_ops(_, _) -> {noop, noop}.

require_state_downstream(_) -> false.

add(Wordcount, File) ->
    Split = binary:split(File, [<<"\n">>, <<" ">>], [global]),
    lists:foldl(fun(Word, Acc) ->
        case maps:is_key(Word, Acc) of
            true ->
                maps:update_with(Word, fun(V) -> V + 1 end, Acc);
            false ->
                maps:put(Word, 1, Acc)
        end
    end, Wordcount, Split).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual(new(), #{}).

file_test() ->
    Wc = new(),
    Wc1 = add(Wc, <<"foo bar baz baz">>),
    ?assertEqual(Wc1, #{<<"foo">> => 1, <<"bar">> => 1, <<"baz">> => 2}).

-endif.



