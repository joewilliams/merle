#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin -sasl errlog_type error -boot start_sasl -noshell

main(_) ->
    etap:plan(7),
    etap_can:loaded_ok(merle, "Module 'merle' loaded"),
    {ok, MerlePid} = merle:start_link("localhost", 11211),
    etap:ok(erlang:is_process_alive(MerlePid), "Merle running"),
    Key = rnd_key(),
    Key2 = rnd_key(),
    etap:is(merle:set(Key, "1", "0", "bar"), ok, "Set data"),
    etap:is(merle:get(Key), "bar", "Get data"),
    etap:is(merle:get(rnd_key()), undefined, "Get invalid data"),
    etap:is(merle:set(Key2, "1", "0", {foo, bar}), ok, "Set data"),
    etap:is(merle:get(Key2), {foo, bar}, "Get data"),
    etap:end_tests().

rnd_key() ->
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
    lists:flatten([
        [[random:uniform(25) + 96] || _ <-lists:seq(1,5)],
        [[random:uniform(9) + 47] || _ <-lists:seq(1,3)]
    ]).
    