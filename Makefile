LIBDIR=`erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell`
APP_NAME="merle"
VSN="0.3"

all: compile

docs:
	erl -noshell -run edoc_run application "'$(APP_NAME)'" '"."' '$(VSN)' -s init stop

compile:
	@mkdir -p ebin
	@erl -make

clean:
	rm -f ebin/*.beam
	rm -f erl_crash.dump

test: all
	prove -v t/*.t

install: all
	mkdir -p ${LIBDIR}/${APP_NAME}-${VSN}/ebin
	for i in ebin/*.beam; do install $$i $(LIBDIR)/${APP_NAME}-${VSN}/$$i ; done
