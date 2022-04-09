module KeyManager

using HiRedis
using DataFrames

function initialize_key_manager(host::String, port::Int)
    start_session(host, port)
end

function get_new_customer_key()
    incr("last_customer_key")
end

function get_new_account_key()
    incr("last_account_key")
end

function get_new_transaction_key()
    incr("last_transaction_key")
end

function get_new_interaction_key()
    incr("last_interaction_key")
end

function get_new_customer_key_range(n::Int)
    if n < 1
        error("n must be a positive integer value")
    end
    if n == 1
        return get_new_customer_key()
    end
    last_customer_key = kvget("last_customer_key")
    key_range_start = last_customer_key == nothing ? 1 : Int(last_customer_key) + 1
    key_range_end = key_range_start + n - 1
    incrby("last_customer_key", n)
    key_range_start:key_range_end
end

function set_customer_attribute(customer_key::String, attr_name::String, attr_value::Any)
    hset(string("customer:", customer_key), attr_name, attr_value)
end

function set_customer_attributes(customer_key::String, attrs::AbstractVector{<: Any})
    hmset(string("customer:", customer_key), attrs)
end

function get_customer_attribute(customer_key::String, attr_name::String)
    hget(string("customer:", customer_key), attr_name)
end

function get_customer_attribute_selection(customer_key::String, fields::Array{String,1})
    hmget(string("customer:", customer_key), fields)
end

function get_customer_attributes(customer_key::String)
    hgetall(string("customer:", customer_key))
end

function set_customer_since_date(customer_key::String, since_date::String)
    set_customer_attribute(customer_key, "since_date", since_date)
end

function get_customer_since_date(customer_key::String)
    get_customer_attribute(customer_key, "since_date")
end

function get_new_account_key_range(n::Int)
    if n < 1
        error("n must be a positive integer value")
    end
    if n == 1
        return get_new_account_key()
    end
    last_account_key = kvget("last_account_key")
    key_range_start = last_account_key == nothing ? 1 : parse(Int, last_account_key) + 1
    key_range_end = key_range_start + n - 1
    incrby("last_account_key", n)
    key_range_start:key_range_end
end

function get_new_transaction_key_range(n::Int64)
    if n < 1
        error("n must be a positive integer value")
    end
    if n == 1
        return get_new_transaction_key()
    end
    last_transaction_key = kvget("last_transaction_key")
    key_range_start = last_transaction_key == nothing ? 1 : parse(Int, last_transaction_key) + 1
    key_range_end = key_range_start + n - 1
    incrby("last_transaction_key", n)
    key_range_start:key_range_end
end

function get_new_interaction_key_range(n::Int64, customer_key)
    if n < 1
        error("n must be a positive integer value")
    end
    if n == 1
        return get_new_interaction_key()
    end
    last_interaction_key = kvget("last_interaction_key")
    key_range_start = last_interaction_key == nothing ? 1 : parse(Int, last_interaction_key) + 1
    key_range_end = key_range_start + n - 1
    incrby("last_interaction_key", n)
    for interaction_key in key_range_start:key_range_end
        sadd(string("interaction_keyset:", customer_key), interaction_key)
    end
    key_range_start:key_range_end
end

function get_customer_key_range()
    1:kvget("last_customer_key")
end

function get_new_account_keys(customer_key::String, account_types::Array{String,1})
    n = length(account_types)
    account_key_range = get_new_account_key_range(n)
    for account_key in account_key_range
        sadd(string("account_keyset:", customer_key), account_key)
    end
    for i in 1:n
        account_key = string(account_key_range[i])
        set_account_attributes(account_key, [
                                   "customer_key", customer_key,
                                   "account_type", account_types[i]
                                   ])
    end
    account_key_range
end

function set_account_attribute(account_key::String, attr_name::String, attr_value::Any)
    hset(string("account:", account_key), attr_name, attr_value)
end

function set_account_attributes(account_key::String, attrs::AbstractVector{<: Any})
    hmset(string("account:", account_key), attrs)
end

function get_account_attribute(account_key::String, attr_name::String)
    hget(string("account:", account_key), attr_name)
end

function get_account_attribute_selection(account_key::String, fields::Array{String,1})
    @debug("account_key", account_key)
    @debug("fields", fields)
    hmget(string("account:", account_key), fields)
end

function get_account_attributes(account_key::String)
    hgetall(string("account:", account_key))
end

function set_account_open_date(account_key::String, open_date::String)
    set_account_attribute(account_key, "open_date", open_date)
end

function get_account_open_date(account_key::String)
    get_account_attribute(account_key, "open_date")
end

function set_account_open_dates(account_keys::Array{String,1}, open_dates::Array{String,1})
    for i in 1:length(account_keys)
        set_account_open_date(account_keys[i], open_dates[i])
    end
end

function get_customer_account_keys(customer_key::String)
    smembers(string("account_keyset:", customer_key))
end

function get_customer_interaction_keys(customer_key::String)
    smembers(string("interaction_keyset:", customer_key))
end

function get_account_open_dates(customer_key::String)
    open_dates = String[]
    account_keys = get_customer_account_keys(customer_key)
    for account_key in account_keys
        open_date = get_account_open_date(account_key)
        push!(open_dates, open_date)
    end
    DataFrame(account_key = account_keys, account_type = account_types, open_date = open_dates)
end

function get_account_type(account_key::String)
    get_account_attribute(account_key, "account_type")
end

function get_customer_accounts(customer_key::String)
    account_keys = get_customer_account_keys(customer_key)
    account_types = String[]
    open_dates = String[]
    @debug("account_keys", account_keys)
    for account_key in account_keys
        @debug("account_key", account_key)
        account_type, open_date = get_account_attribute_selection(account_key, ["account_type", "open_date"])
        push!(account_types, account_type)
        push!(open_dates, open_date)
    end
    DataFrame(account_key = account_keys, account_type = account_types, open_date = open_dates)
end

function add_to_set(set_name::String, argv::Any...)
    sadd(set_name, argv...)
end

function get_members(set_name::String)
    smembers(set_name)
end

function get_intersection(argv::String...)
    sinter(argv...)
end

function get_diff(set_name::String, other_sets::String...)
    sdiff(set_name, other_sets...)
end

export initialize_key_manager, get_new_customer_key, get_new_account_key, get_new_transaction_key,
    get_new_interaction_key, get_new_customer_key_range, set_customer_attribute,
    set_customer_attributes, get_customer_attribute, get_customer_attribute_selection,
    get_customer_attributes, set_customer_since_date, get_customer_since_date,
    get_new_account_key_range, get_new_transaction_key_range, get_new_interaction_key_range,
    get_customer_key_range, get_new_account_keys, set_account_attribute, set_account_attributes,
    get_account_attribute, get_account_attribute_selection, get_account_attributes,
    set_account_open_date, get_account_open_date, set_account_open_dates, get_customer_account_keys,
    get_account_open_dates, get_account_type, get_customer_accounts, get_customer_interaction_keys,
    add_to_set, get_members, get_intersection, get_diff

end
