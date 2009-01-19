#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin -sasl errlog_type error -boot start_sasl -noshell

main(_) ->
    etap:plan(9),
    etap_can:loaded_ok(merle, "Module 'merle' loaded"),
    etap_can:can_ok(merle, stats),
    etap_can:can_ok(merle, version),
    etap_can:can_ok(merle, get),
    etap_can:can_ok(merle, delete),
    etap_can:can_ok(merle, set),
    etap_can:can_ok(merle, add),
    etap_can:can_ok(merle, replace),
    etap_can:can_ok(merle, cas),
    etap:end_tests().
