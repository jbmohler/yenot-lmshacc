### Introduction

This is a simple double entry accounting system (server only) built on top of
Yenot server micro-framework.

### Installation & Initialization

The setup piggybacks on top of the Yenot init-database.   A command line like
the following is probably the thing.

~~~~
python yenot/scripts/init-database.py --full-recreate   \
	--ddl-script=lmshacc/schema/lmshacc.sql \
	postgresql:///lmsprod
~~~~
