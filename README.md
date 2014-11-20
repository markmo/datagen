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

## Parallelization

The code can process blocks of data in parallel. This may be required for performance or to limit the number of records held in memory. For example, to generate 4000 customers and their associated records, the following parameters can be set in src/run.jl:

    num_customers = 4000
    block_size = 1000

This will process 4 blocks of 1000 customers each in parallel, writing separate files, e.g.

    data/customers_1.csv
    data/customers_2.csv
    data/customers_3.csv
    data/customers_4.csv

The number of blocks is written to a file in the root directory, "numblocks", which is used by the scripts/datagen.sh script to join them up. To facilitate joining multiple files of records, the header is now written to a separate file:

    data/customers_header.csv

The scripts/test.sh script will perform this step without moving the resulting files to the target server.

The scripts/datagen.sh script will, in addition, perform post processing to add row numbers, as the current process expects. It will also move the final files to the target server.

The datagen script is producing output files named according to what DataStage currently expects, which includes old and specific date suffixes on filenames. DataStage will need to be updated to pick up either consistently named files in a date stamped folder, or to match on the first part of the filename and ignore the date part.

To make use of parallel execution, the following parameter must be added to the julia command when either starting an interactive shell or executing a julia file:

    julia -p 2 src/run.jl

The number following the "-p" option is the number of processes, which Julia may spawn. It generally makes sense to set this to the number of cores (or virtual cores) of the host machine, since this is the true parallelization that can be achieved without time slicing.

The scripts/datagen.sh script sets this number to a default of 4.

### Log file

The datagen shell script also expects to write to a log file in the "/var/log/datagen" directory, which is expected to already exist with write permission granted to the account running the shell script. On rstudio.snbc.io, the scripts are run under an account called "datagen".

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

Next, install the hiredis C client library for Redis. The Julia Redis Client wraps hiredis.

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
    cd datagen

DataGen also depends on the [HiRedis.jl](https://github.com/markmo/HiRedis.jl) package. To install this package, start Julia and run:

    julia> Pkg.clone("HiRedis")

Other package dependencies include:

    Pkg.add("StatsBase")
    Pkg.add("Distributions")
    Pkg.add("DataFrames")
    Pkg.add("Docile")
    Pkg.add("Logging")
    Pkg.add("Compat")        # For Julia version 0.3.x backwards compatibility
    Pkg.add("Dates")         # only if using Julia version 0.3.x

Exit Julia. Edit the parameters at the top of run.jl. Change to the "src" directory, then execute run.jl from the OS command line:

    julia run.jl

The datasets above will be created in the data directory.

To use the app interactively:

    julia

    julia> using DataGen
    julia> using HiRedis
    julia> start_session()
    julia> profiles = get_customer_profiles(10)

See run.jl as an example.

See [Julia documentation](http://julia.readthedocs.org/en/latest/manual/). Also, [Learn X in Y minutes](http://learnxinyminutes.com/docs/julia/) is a quick introduction.
