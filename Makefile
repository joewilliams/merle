all: compile

compile:
	@erl -make

clean:
	rm -f ebin/*.beam
	rm -f erl_crash.dump
