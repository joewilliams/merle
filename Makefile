APP_NAME="merle"
VSN="pre0.1"

all: compile

docs: 
	erl -noshell -run edoc_run application "'$(APP_NAME)'" '"."' '$(VSN)' -s init stop


compile:
	@erl -make

clean:
	rm -f ebin/*.beam
	rm -f erl_crash.dump
