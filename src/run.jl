srcdir = dirname(@__FILE__)
require(string(srcdir, "/DataGen.jl"))

using DataGen
using DataFrames
using Hiredis
using KeyManager

num_customers = 500
block_size = 500
enddate = "2014-11-12"
balance_days = 30
transaction_days = 60
redis_host = "127.0.0.1"
redis_port = 6379

start_session(redis_host, redis_port)
flushall()

rem = 0
n = 0
if num_customers < block_size
    rem = num_customers
    n = 1
else
    rem = num_customers % block_size
    if rem == 0
        rem = block_size
        n = int(num_customers / block_size)
    else
        n = int((num_customers - rem) / block_size) + 1
    end
end

customer_accounts = DataFrame()

function head(a::Array)
    a[1]
end

function tail(a::Array)
    a[2:end]
end

# Array{Any,1} doesn't work - ERROR: `makestring` has no method matching makestring(::Array{Symbol,1})
function makestring(a::Array)
    function makestring_(a::Array)
        quotechar = '"'
        sepchar = ','
        if isempty(a)
            ""
        else
            string(quotechar, string(head(a)), quotechar, sepchar, makestring_(tail(a)))
        end
    end
    if isempty(a)
        ""
    else
        makestring_(a)[1:end-1]
    end
end

function generate(i::Int, num_customers::Int, block_size::Int, enddate::ASCIIString, balance_days::Int, transaction_days::Int; writeheaders::Bool=false)
    persons, mail_addresses, residential_addresses, customers = create_customer_profiles(num_customers, enddate)
    customer_keys = persons[:customer_sk]

    accounts = DataFrame()
    channel_usage = DataFrame()
    interactions = DataFrame()
    interaction_keys = ASCIIString[]
    interaction_customer_keys = ASCIIString[]

    for key in customer_keys
        customer_key = string(key)
        account_holdings = create_account_holdings(customer_key)
        global customer_accounts = rbind(customer_accounts, account_holdings)
        account_details = create_account_details(customer_key)
        accounts = rbind(accounts, account_details)

        usage = create_channel_usage(customer_key, enddate, balance_days)
        channel_usage = rbind(channel_usage, usage)

        ib_yn = get_customer_attribute(customer_key, "ib_yn")
        gm_yn = get_customer_attribute(customer_key, "gomoney_yn")

        if ib_yn == "Y" || gm_yn == "Y"
            interacts = create_interactions(customer_key, enddate, transaction_days)
            interactions = rbind(interactions, interacts)
        end

        interact_keys = get_customer_interaction_keys(customer_key)
        interact_customer_keys = rep(customer_key, length(interact_keys))
        append!(interaction_keys, interact_keys)
        append!(interaction_customer_keys, interact_customer_keys)
    end

    customer_interactions = DataFrame(
        customer_sk    = Int[int(key) for key in interaction_customer_keys],
        interaction_sk = Int[int(key) for key in interaction_keys]
        )

    account_keys = accounts[:account_sk]

    account_balances = DataFrame()
    account_transactions = DataFrame()

    for key in account_keys
        account_key = string(key)
        account_type_code = get_account_type(account_key)
        account_balance = create_account_balance(account_key, account_type_code, enddate, balance_days)
        account_balances = rbind(account_balances, account_balance)
        account_transaction = create_transactions(account_key, account_type_code, enddate, transaction_days)
        if account_transaction != nothing
            account_transactions = rbind(account_transactions, account_transaction)
        end
    end

    joint_accounts = create_joint_accounts(customer_accounts)
    global customer_accounts = rbind(customer_accounts, joint_accounts)

    if writeheaders
        open(string(srcdir, "/../data/persons_header.csv"), "w") do f; println(f, makestring(names(persons))); end;
        open(string(srcdir, "/../data/mail_addresses_header.csv"), "w") do f; println(f, makestring(names(mail_addresses))); end;
        open(string(srcdir, "/../data/residential_addresses_header.csv"), "w") do f; println(f, makestring(names(residential_addresses))); end;
        open(string(srcdir, "/../data/customers_header.csv"), "w") do f; println(f, makestring(names(customers))); end;
        open(string(srcdir, "/../data/account_balances_header.csv"), "w") do f; println(f, makestring(names(account_balances))); end;
        open(string(srcdir, "/../data/account_transactions_header.csv"), "w") do f; println(f, makestring(names(account_transactions))); end;
        open(string(srcdir, "/../data/channel_usage_header.csv"), "w") do f; println(f, makestring(names(channel_usage))); end;
        open(string(srcdir, "/../data/customer_accounts_header.csv"), "w") do f; println(f, makestring(names(customer_accounts))); end;
        open(string(srcdir, "/../data/accounts_header.csv"), "w") do f; println(f, makestring(names(accounts))); end;
        open(string(srcdir, "/../data/interactions_header.csv"), "w") do f; println(f, makestring(names(interactions))); end;
        open(string(srcdir, "/../data/customer_interactions_header.csv"), "w") do f; println(f, makestring(names(customer_interactions))); end;
    end

    writetable(string(srcdir, "/../data/persons_", i, ".csv"), persons; header=false)
    writetable(string(srcdir, "/../data/mail_addresses_", i, ".csv"), mail_addresses; header=false)
    writetable(string(srcdir, "/../data/residential_addresses_", i, ".csv"), residential_addresses; header=false)
    writetable(string(srcdir, "/../data/customers_", i, ".csv"), customers; header=false)
    writetable(string(srcdir, "/../data/account_balances_", i, ".csv"), account_balances; header=false)
    writetable(string(srcdir, "/../data/account_transactions_", i, ".csv"), account_transactions; header=false)
    writetable(string(srcdir, "/../data/channel_usage_", i, ".csv"), channel_usage; header=false)
    writetable(string(srcdir, "/../data/customer_accounts_", i, ".csv"), customer_accounts; header=false)
    writetable(string(srcdir, "/../data/accounts_", i, ".csv"), accounts; header=false)
    writetable(string(srcdir, "/../data/interactions_", i, ".csv"), interactions; header=false)
    writetable(string(srcdir, "/../data/customer_interactions_", i, ".csv"), customer_interactions; header=false)
end

for i in 1:n
    num_cust = i < n ? block_size : rem
    writeheaders = i == 1
    generate(i, num_cust, block_size, enddate, balance_days, transaction_days; writeheaders=writeheaders)
end

open(string(srcdir, "/../numblocks"), "w") do f
    print(f, string(n))
end
