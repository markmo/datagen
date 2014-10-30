# CAA DataGen

Synthetic data generation for the customer analytic asset (CAA). The DataGen scripts will create the following datasets with data that has realistic distributions of values:

* persons
* mail_addresses
* residential_addresses
* customers
* account_balances
* account_transactions
* channel_usage
* customer_accounts
* accounts
* interactions

The code is written in Julia after being prototypes in R. Julia has the expressiveness of R, to create data that has a realistic shape, and the speed of C, to generate large data sets (millions of rows) quickly.

Redis, an in-memory database, has also been used to cache values during generation to ensure referential integrity between datasets.

## Setup

These instructions are for Unix / OS/X.

The following system dependencies are required. The Homebrew package manager has been used.

To install Homebrew:

    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

First of all, to install Julia:

	brew update
	brew tap staticfloat/julia
	brew install --HEAD julia
	brew test -v --HEAD julia

If this fails, gcc may not be installed or out of date.

	brew rm gcc
	brew install gcc

If you already have R installed using Homebrew, then you may need to reinstall it as it may reference an older version of gfortran.

	brew rm R
	brew install R

Next, install hiredis. Hiredis is the C client to Redis. The Julia Redis Client wraps hiredis.

	brew install hiredis

Install Redis.

	brew install redis

To start redis, open a new command window and run:

	redis-server

You can open a command line connection to the Redis Server to test it independently by opening a new command window and running:

	redis-cli

The default Redis port is 6379. See the [Redis website](http://redis.io/).

Clone this app locally. Change to the src subdirectory. For example:

	git clone http://<YOUR USERNAME>@stash.snbc.io/scm/col/datagen.git
	cd datagen/src

Currently, you must create a "data" subdirectory under "src".

	mkdir <PROJECT HOME>/src/data

Edit the parameters at the top of run.jl then execute:

	julia run.jl

The datasets above will be created in the data directory.

To use the app interactively:

	julia

	julia> using DataGen
	julia> using RedisClient
	julia> start_session()
	julia> profiles = get_customer_profiles(10)

See run.jl as an example.

See [Julia documentation](http://julia.readthedocs.org/en/latest/manual/). Also, [Learn X in Y minutes](http://learnxinyminutes.com/docs/julia/) is a quick introduction.
