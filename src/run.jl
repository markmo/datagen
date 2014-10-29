using DataGen
using DataFrames
using RedisClient
using KeyManager

num_customers = 1000
enddate = "2014-10-31"
balance_days = 30
transaction_days = 60
redis_host = "127.0.0.1"
redis_port = 6379

start_session(redis_host, redis_port)

profiles = get_customer_profiles(num_customers, enddate)
persons = profiles["persons"]
customer_keys = persons[:customer_sk]

customer_accounts = DataFrame()
accounts = DataFrame()
channel_usage = DataFrame()
interactions = DataFrame()
interaction_keys = []
interaction_customer_keys = []

for customer_key in customer_keys
    account_holding = get_account_holdings(customer_key)
    customer_accounts = rbind(customer_accounts, account_holding)
    account = get_account_details(customer_key, enddate)
    accounts = rbind(accounts, account)

    usage = get_channel_usage(customer_key, enddate, balance_days)
    channel_usage = rbind(channel_usage, usage)

    ib_yn = get_customer_attribute("customer_key", "ib_yn")
    gm_yn = get_customer_attribute("customer_key", "gomoney_yn")

    if ib_yn == "Y" || gm_yn == "Y"
        interacts = get_interactions(customer_key, enddate, transaction_days)
        interactions = rbind(interactions, interacts)
    end

    interact_keys = get_customer_interaction_keys(customer_key)
    interact_customer_keys = [customer_key for i = 1:length(interact_keys)]
    push!(interaction_keys, interact_keys)
    push!(interaction_customer_keys, interact_customer_keys)
end

customer_interactions = DataFrame(
    customer_sk    = interaction_customer_keys,
    interaction_sk = interaction_keys
    )

account_keys = accounts[:account_sk]

account_balances = DataFrame()
account_transactions = DataFrame()

for account_key in account_keys
    account_balance = get_account_balance(account_key, enddate, balance_days)
    account_balances = rbind(account_balances, account_balance)
    account_transaction = get_transactions(account_key, enddate, transaction_days)
    if account_transaction != nothing
        account_transactions = rbind(account_transactions, account_transaction)
    end
end

writetable("data/persons.csv", profiles["persons"])
writetable("data/mail_addresses.csv", profiles["mail_addresses"])
writetable("data/residential_addresses.csv", profiles["residential_addresses"])
writetable("data/customers.csv", profiles["customers"])
writetable("data/account_balances.csv", account_balances)
writetable("data/account_transactions.csv", account_transactions)
writetable("data/channel_usage.csv", channel_usage)
writetable("data/customer_accounts.csv", customer_accounts)
writetable("data/accounts.csv", accounts)
writetable("data/interactions.csv", interactions)
