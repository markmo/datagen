module DataGen

# srcdir = dirname(@__FILE__)
include("KeyManager.jl")

using .KeyManager
using CSV
using StatsBase
using Distributions
using DataFrames
using HiRedis
using Logging
using Compat
using Dates

if VERSION <= v"0.3"
using Dates
end

logger = SimpleLogger(stdout, Logging.Debug)
global_logger(logger)

# struct rep{T} end
# function rep{T}(var::T, n::Int64) where T
#     T[var for i = 1:n]
# end

function rep(var::T, n::Int) where T <: Any
    T[var for i = 1:n]
end

function samp(ary::Union{Array,UnitRange{Int64}}, n::Int=1)
    if eltype(ary) <: String
        a = sample(ary, n)
    else
        a = sample(collect(Iterators.flatten(ary)), n)
    end
    if size(a)[1] == 1
        a[1]
    else
        a
    end
end

function create_since_date(enddate::String; startdate::String="1970-01-01", min_days::Int=100, min_age_years::Int=5, n::Int=1)
    st = Date(startdate) + Dates.Year(min_age_years)
    et = Date(enddate) - Dates.Day(min_days)
    dt = Dates.value(et - st)
    ev = sort(rand(1:dt, n))
    dates = fill("", n)
    for i = 1:n
        d = st + Dates.Day(ev[i])
        dates[i] = string(d)
    end
    if n == 1
        dates[1]
    else
        dates
    end
end

const branches = @compat Dict{String,Array{String,1}}(
    "VIC" => ["Bourke St Mall", "South Yarra", "Toorak", "Northcote", "Moonee Ponds", "Footscray Mall"],
    "NSW" => ["242 Pitt St", "Chifley Square", "Darlinghurst", "Double Bay", "Crows Nest", "Neutral Bay"],
    "QLD" => ["West End", "Fortitude Valley", "Woolloongabba", "Carnidale", "Mount Gravatt Cental", "Wynnum"],
    "WA"  => ["Allendale Square", "Northbridge", "West Perth", "Leederville", "Mount Lawley"],
    "SA"  => ["Hutt St", "Gouger St", "Rundell Mall", "North Adelaide", "Norwood"],
    "ACT" => ["Canberra Centre", "Dickson", "Manuka", "Fyshwick"],
    "NT"  => ["Darwin", "Palmerston", "Winnellie", "Casuarina"]
    )

function create_domicile_branch(state::String)
    samp(branches[state])
end

function create_credit_behavioural_risk_rating()
    samp([
        samp(550:599, 15),
        samp(600:649, 12),
        samp(650:699, 15),
        samp(700:749, 18),
        samp(750:799, 27),
        samp(800:850, 13)
      ])
end

const male_first_names = ["Bob", "Jim", "Richard", "David"]

const female_first_names = ["Alice", "Jane", "Lynley", "Ellen"]

function create_first_name(gender::String)
    if gender == "M"
        samp(male_first_names)
    else
        samp(female_first_names)
    end
end

const last_names = ["Smith", "Jones", "Wong", "Khan", "Diaz", "Papadakis", "Costa", "Williams", "Nguyen", "Harris"]

function create_last_name()
    samp(last_names)
end

function create_name_prefix(gender::String)
    if gender == "M"
        "Mr"
    else
        samp(["Ms", "Mrs"])
    end
end

function create_occupation(occupation_code::String)
    if occupation_code == "0001"
        "Accountant"
    elseif occupation_code == "0006"
        "Engineer"
    else
        "Factory Worker"
    end
end

function create_birth_date(enddate::String)
    age = samp([
        samp(20:24, 96),
        samp(25:29, 99),
        samp(30:34, 102),
        samp(35:39, 105),
        samp(40:44, 109),
        samp(45:49, 99),
        samp(50:54, 94),
        samp(55:59, 85),
        samp(60:64, 79),
        samp(65:69, 62),
        samp(70:74, 70),
    ])
    ed = Date(enddate) - Dates.Year(age)
    string(ed)
end

function create_employment_status()
    status = samp([rep("FT", 9), "PT"])
    if status == "FT"
        "Full time"
    else
        "Part time"
    end
end

const employers = ["HR Block", "BHP Bilton", "Rio Tinto", "Telstra", "Wesfarmers", "Woolworths", "Brown Brothers", "Bulla Dairy Foods", "Coca-Cola Amatil", "Foster's Group"]

function create_employer()
    samp(employers)
end

const prefixes = @compat Dict{String,String}("VIC" => "03", "NSW" => "02", "QLD" => "07", "ACT" => "06", "SA" => "08", "WA" => "08", "NT" => "08", "TAS" => "03")

function create_phone(state::String)
    string(prefixes[state], string(rand())[3:10])
end

function create_mobile()
    string("04", string(rand())[3:10])
end

"""Returns an Australian state with a realistic frequency distribution."""
function create_state()
    samp([
        rep("VIC", 25),
        rep("NSW", 32),
        rep("QLD", 20),
        rep("WA", 11),
        rep("SA", 7),
        rep("ACT", 2),
        ["NT"]])
end

const postcodes = @compat Dict{String,String}(
    "VIC" => "3121",
    "NSW" => "2089",
    "QLD" => "4014",
    "WA"  => "6025",
    "SA"  => "5035",
    "ACT" => "2600",
    "NT"  => "0801"
    )

function create_postcode(state::String)
    postcodes[state]
end

const street_names = ["Eskdale", "Queen", "Johnson", "Egan", "Rolleston", "Smith"]

function create_street_name()
  samp(street_names)
end

const street_types = ["Street", "Road", "Place", "Avenue", "Parade", "Lane", "Crescent", "Close"]

function create_street_type()
  samp(street_types)
end

const occupation_codes = ["0001", "0006", "0059"]

const contact_methods = ["marketing email", "home phone", "mobile phone", "work phone"]

const address_types = ["mail", "residential", "residential"]

"""
Generates the t_Person table.

* gender The gender of the person
* state An Australian state (standard abbreviations). Used to generate the area codes of the phone numbers associated with the customer.
* customer_key A record id number, used as the surrogate key.
* enddate The final date for data generation for the customer asset.
"""
function create_person(gender::String, state::String, customer_key::String, enddate::String)
    occupation_code = samp(occupation_codes)
    first_name = create_first_name(gender)
    last_name = create_last_name()
    preferred_contact_method = samp(contact_methods)
    home_phone = create_phone(state)
    mobile_phone = create_mobile()
    work_phone = create_phone(state)
    preferred_email = missing
    preferred_phone = missing
    if preferred_contact_method == "marketing email"
        preferred_email = string(first_name, ".", last_name, "@gmail.com")
    elseif preferred_contact_method == "home phone"
        preferred_phone = home_phone
    elseif preferred_contact_method == "mobile phone"
        preferred_phone = mobile_phone
    elseif preferred_contact_method == "work phone"
        preferred_phone = work_phone
    end
    DataFrame(
        customer_sk                     = parse(Int, customer_key),
        first_name                      = first_name,
        last_name                       = last_name,
        preferred_name                  = first_name,
        name_prefix                     = create_name_prefix(gender),
        name_suffix                     = missing,
        occupation_cd                   = occupation_code,
        occupation                      = create_occupation(occupation_code),
        gender_cd                       = gender,
        birth_dt                        = create_birth_date(enddate),
        deceased_dt                     = "", #missing,
        employment_status               = create_employment_status(),
        employer_name                   = create_employer(),
        months_with_current_employer    = samp(0:200),
        home_phone                      = home_phone,
        mobile_phone                    = mobile_phone,
        work_phone                      = work_phone,
        preferred_contact_method        = preferred_contact_method,
        preferred_email                 = preferred_email,
        preferred_phone                 = preferred_phone,
        preferred_address_type          = samp(address_types)
        )
end

"""
Generates the t_Mail_Contact or t_Residential_Contact table.

* state An Australian state (standard abbreviations). Used as the state for the address record and to generate a realistic postcode.
* customer_key A record id number, used as the surrogate key.
"""
function create_address(state::String, customer_key::String)
    DataFrame(
        customer_sk                     = parse(Int, customer_key),
        postcode                        = create_postcode(state),
        street_name                     = create_street_name(),
        street_type                     = create_street_type(),
        state                           = state,
        country                         = "AU",
        dpid                            = missing,
        address_status_cd               = "A",
        address_status_desc             = "Active"
        )
end

"""
Generates the t_Customer table.

* person
* state An Australian state (standard abbrevations). Used to generate the domicled branch of the customer.
* enddate The final date for data generation for the customer asset.
* customer_key A record id number, used as the surrogate key.
"""
function create_customer(person::DataFrame, state::String, enddate::String, customer_key::String)
    customer_since_date = create_since_date(enddate, startdate=person[1,:birth_dt])
    set_customer_since_date(customer_key, customer_since_date)
    DataFrame(
        customer_sk                     = parse(Int, customer_key),
        cap_cis_id                      = parse(Int, customer_key),
        customer_type_cd                = "R",
        customer_type_desc              = "Retail",
        customer_status_cd              = "01",
        customer_status_desc            = "Active",
        customer_since_date             = customer_since_date,
        customer_left_dt                = "", #missing,
        customer_bankrupt_dt            = "", #missing,
        customer_domicile_branch_cd     = missing,
        customer_domicile_branch_desc   = create_domicile_branch(state),
        relationship_manager_cd         = missing,
        relationship_manager            = missing,
        rlnshp_manager_assigned_dt      = create_since_date(enddate, startdate="2005-01-01", min_days=0, min_age_years=0),
        contact_allowed_yn              = "Y",
        credit_behavioural_risk_rating  = create_credit_behavioural_risk_rating(),
        relationship_depth_flag         = missing,
        annual_revenue                  = 0,
        annual_cost_to_serve            = 0,
        marketing_email                 = person[1,:preferred_email],
        statement_email                 = person[1,:preferred_email],
        bpay_email                      = person[1,:preferred_email]
        )
end

"""
Generates the t_Person, t_Customer, t_Mail_Contact and t_Residential_Contact tables.

* n The number of profiles to generate
* enddate The final date for data generation for the customer asset.
"""
function create_customer_profiles(n::Int=1000, enddate::String="2014-10-31")
    customer_key_range = get_new_customer_key_range(n)
    mail_addresses = DataFrame()
    residential_addresses = DataFrame()
    persons = DataFrame()
    customers = DataFrame()
    for i in 1:n
        customer_key = string(customer_key_range[i])
        gender = samp(["M", "F"])
        state = create_state()
        set_customer_attribute(string("customer:", customer_key), "state", state)
        add_to_set(string("state:", state), customer_key)
        person = create_person(gender, state, customer_key, enddate)
        persons = [persons; person]
        mail_address = create_address(state, customer_key)
        mail_addresses = [mail_addresses; mail_address]
        residential_address = create_address(state, customer_key)
        residential_addresses = [residential_addresses; residential_address]
        customer = create_customer(person, state, enddate, customer_key)
        customers = [customers; customer]
    end
    persons, mail_addresses, residential_addresses, customers
end

function create_customer_account_role_code(account_type_code::String)
    if account_type_code == "CC"
        "PR"
    else
        "SO"
    end
end

function create_customer_account_role_desc(account_type_code::String)
    if account_type_code == "CC"
        "Primary"
    else
        "Sole"
    end
end

"""
Generates t_Customer_Account.

* customer_key A record id number, used as the surrogate key.
"""
function create_account_holdings(customer_key::String)
    num_accounts = samp([rep(1, 11), rep(2, 3), rep(3, 6), rep(4, 6), rep(5, 6)])
    if num_accounts == 1
        account_type_codes = [samp([rep("CC", 73), rep("TXN", 9), rep("PL", 9), rep("TD", 9)])]
    elseif num_accounts == 2
        account_type_codes = collect(Iterators.flatten([["TXN"], [samp(["TD", "TD", "PL"])]]))
    elseif num_accounts == 3
        account_type_codes = collect(Iterators.flatten([["TXN", "CC"], [samp(["SV", "SV", "MT"])]]))
    elseif num_accounts == 4
        account_type_codes = collect(Iterators.flatten([["TXN", "CC", "MT"], [samp(["SV", "MT", "CC"])]]))
    elseif num_accounts == 5
        account_type_codes = collect(Iterators.flatten([["TXN", "CC", "MT", "SV"], [samp([["TD"], rep("MT", 3)])]]))
    end
    customer_account_role_codes = [create_customer_account_role_code(account_type_codes[i]) for i in 1:num_accounts]
    customer_account_role_descs = [create_customer_account_role_desc(account_type_codes[i]) for i in 1:num_accounts]
    a = get_new_account_keys(customer_key, account_type_codes)
    if typeof(a) == Int64
        a = [a]
    end
    account_keys = String[string(key) for key in a]
    customer_since_date = get_customer_since_date(customer_key)
    year = Dates.year(Date(customer_since_date))
    add_to_set(string("year:", year), customer_key)
    account_open_date = Date(year, 12, 31)
    set_account_open_dates(account_keys, rep(string(account_open_date), num_accounts))
    for i = 1:length(account_keys)
        add_to_set(string("account_type:", account_type_codes[i]), account_keys[i])
    end
    DataFrame(
        customer_sk                     = rep(parse(Int, customer_key), num_accounts),
        account_sk                      = Int[parse(Int, key) for key in account_keys],
        customer_account_role_start_dt  = rep(customer_since_date, num_accounts),
        customer_account_role_end_dt    = rep("", num_accounts), #rep(missing, num_accounts),
        customer_account_role_cd        = customer_account_role_codes,
        customer_account_role_desc      = customer_account_role_descs,
        account_held_since_dt           = rep(account_open_date, num_accounts)
        )
end

function create_account_type_descs(account_type_codes::Array{String,1})
    account_types = @compat Dict{String,String}(
        "MT" => "Mortgage",
        "CC" => "Credit Card",
        "TD" => "Term Deposit",
        "PL" => "Personal Loan",
        "TXN" => "Transaction Account",
        "SV" => "Savings Account"
        )
    [get!(account_types, code, "Not set") for code in account_type_codes]
end

function create_num_transactions(account_type_code::String, days::Int)
    if account_type_code in ["CC", "TXN"]
        samp(1:5, days)
    elseif account_type_code in ["MT", "PL"]
        samp([rep(0, days - 1), 1], days)
    elseif account_type_code == "SV"
        samp([1, 0, 0], days)
    elseif account_type_code == "TD"
        samp([1, rep(0, 50)], days)
    end
end

function create_sign(account_type_code::String)
    if account_type_code in ["MT", "PL"]
        "DR"
    elseif account_type_code == "TD"
        samp(["CR", "CR", "DR"])
    elseif account_type_code in ["CC", "TXN"]
        samp([["CR"], rep("DR", 28)])
    elseif account_type_code == "SV"
        samp([rep("CR", 20), ["DR"]])
    end
end

function create_amount(account_type_code::String, transaction_dr_cr::String)
    if account_type_code == "MT"
        samp(-400000:-100000) / 100
    elseif account_type_code == "TD" && transaction_dr_cr == "DR"
        samp([-1000, -2000, -3000, -5000, -7500])
    elseif account_type_code == "TD" && transaction_dr_cr == "CR"
        samp([1000, 2000, 3000, 5000, 7500, samp(8000:30000) / 100])
    elseif account_type_code == "TXN" && transaction_dr_cr == "DR"
        samp([[-round(rand(Normal(10., 3.)), digits=2) for i = 1:5], -round(rand(Normal(110., 40.)), digits=2)])
    elseif account_type_code == "TXN" && transaction_dr_cr == "CR"
        samp([1100, 2200, 3300, 4400, 5500, 6600, 7700, 8800, 9900])
    elseif account_type_code == "CC" && transaction_dr_cr == "DR"
        samp(100000:300000) / 100
    elseif account_type_code == "CC" && transaction_dr_cr == "CR"
        samp([[-round(rand(Normal(20., 6.)), digits=2) for i = 1:5], -round(rand(Normal(200., 40.)), digits=2)])
    elseif account_type_code == "SV"
        samp(50:250)
    elseif account_type_code == "PL"
        samp(-25000:5000) / 100
    end
end

function create_opening_balance(account_type_code::String)
    if account_type_code == "MT"
        samp(10000000:90000000) / 100
    elseif account_type_code == "TD"
        samp(500000:50000000) / 100
    elseif account_type_code == "TXN"
        samp(500000:2000000) / 100
    elseif account_type_code == "CC"
        samp(200000:1500000) / 100
    elseif account_type_code == "SV"
        samp(500000:20000000) / 100
    elseif account_type_code == "PL"
        samp(500000:8000000) / 100
    end
end

"""
Generates the t_Account table.

* customer_key A record id number, used as the surrogate key.
* enddate The final date for data generation for the customer asset.
"""
function create_account_details(customer_key::String)
    core_customer_details = get_customer_accounts(customer_key)
    DataFrame(
        account_sk                      = Int[parse(Int, key) for key in core_customer_details[!, :account_key]],
        account_id                      = Int[parse(Int, key) for key in core_customer_details[!, :account_key]],
        account_type_cd                 = core_customer_details[!, :account_type],
        account_type_desc               = create_account_type_descs(convert(Array, core_customer_details[!, :account_type])),
        account_open_dt                 = core_customer_details[!, :open_date],
        account_close_dt                = "", #missing,
        account_status_cd               = "A",
        account_status_desc             = "Active"
        )
end

"""
Generates the t_Account_Balance table.

* customer_key A record id number, used as the surrogate key.
* enddate The final date for data generation for the customer asset.
* days The number of days to generate data for.
"""
function create_account_balance(account_key::String, account_type_code::String, enddate::String, days::Int)
    dt = Date(enddate) - Dates.Day(days)
#     account_type_code = get_account_type(account_key)
    opening_balance = create_opening_balance(account_type_code)
    DataFrame(
        account_sk                      = rep(parse(Int, account_key), days),
        account_balance_type_cd         = rep(missing, days),
        account_balance_type_desc       = rep(missing, days),
        account_balance_dt              = [dt + Dates.Day(i) for i = 0:(days - 1)],
        account_balance_val             = collect(Iterators.flatten([[opening_balance], rep(0, days - 1)]))
        )
end

"""
Generates the t_Account_Transactions table.

* customer_key A record id number, used as the surrogate key.
* enddate The final date for data generation for the customer asset.
* days The number of days to generate data for.
"""
function create_transactions(account_key::String, account_type_code::String, enddate::String, days::Int)
#     account_type_code = get_account_type(account_key)
    transactions_per_day = create_num_transactions(account_type_code, days)
    num_transactions = sum(transactions_per_day)
    times = String[]
    amounts = Int[]
    dt = Date(enddate) - Dates.Day(days + 1)
    for i in 1:days
        dt = dt + Dates.Day(1)
        if transactions_per_day[i] > 0
            for j in 1:transactions_per_day[i]
                ts = string(dt)
                sign = create_sign(account_type_code)
                amount = round(Int, create_amount(account_type_code, sign))
                push!(times, ts)
                push!(amounts, amount)
            end
        end
    end
    if num_transactions > 0
        DataFrame(
            transaction_sk                = Int[Int(key) for key in get_new_transaction_key_range(num_transactions)],
            transaction_ts                = times,
            transaction_amount            = amounts,
            transaction_channel_type_cd   = rep(missing, num_transactions),
            transaction_channel_type_desc = rep(missing, num_transactions),
            transaction_dr_cr             = rep(missing, num_transactions),
            transaction_ref_num           = rep(missing, num_transactions),
            transaction_status_cd         = rep(missing, num_transactions),
            transaction_status_desc       = rep(missing, num_transactions),
            merchant_id                   = rep(missing, num_transactions),
            account_sk                    = rep(parse(Int, account_key), num_transactions)
            )
    else
        nothing
    end
end

function create_channel_type_codes(ib_yn::String, gomoney_yn::String, num_interactions::Int)
    if ib_yn == "Y" && gomoney_yn == "Y"
        samp(["GM", "IB"], num_interactions)
    elseif ib_yn == "Y" && gomoney_yn == "N"
        rep("IB", num_interactions)
    elseif ib_yn == "N" && gomoney_yn == "Y"
        rep("GM", num_interactions)
    end
end

function create_channel_type_descs(channel_type_codes::Array{String,1})
    [code == "IB" ? "Internet Banking" : "GoMoney" for code in channel_type_codes]
end

function create_num_interactions(days::Int)
    samp([0, 0, 1, 2], days)
end

"""
Generates the t_Interactions table.

* customer_key A record id number, used as the surrogate key.
* enddate The final date for data generation for the customer asset.
* days The number of days to generate data for.
"""
function create_interactions(customer_key::String, enddate::String, days::Int)
    ib_yn = get_customer_attribute(customer_key, "ib_yn")
    gomoney_yn = get_customer_attribute(customer_key, "gomoney_yn")
    interactions_per_day = create_num_interactions(days)
    total_num_interactions = sum(interactions_per_day)
    channel_type_codes = create_channel_type_codes(ib_yn, gomoney_yn, total_num_interactions)
    channel_type_descs = create_channel_type_descs(channel_type_codes)
    start_times = String[]
    end_times = String[]
    start_ts = DateTime(enddate) - Dates.Day(days + 1)
    for i in 1:days
        start_ts = start_ts + Dates.Day(1)
        if interactions_per_day[i] > 0
            for j in 1:interactions_per_day[i]
                end_ts = start_ts + Dates.Day(samp(10:180))
                push!(start_times, string(start_ts))
                push!(end_times, string(end_ts))
            end
        end
    end

    @debug(string("total_num_interactions size:", total_num_interactions))
    @debug(string("start_times size:", length(start_times)))
    @debug(string("end_times size:", length(end_times)))
    @debug(string("channel_type_codes size:", length(channel_type_codes)))
    @debug(string("channel_type_descs size:", length(channel_type_descs)))

    DataFrame(
        interaction_sk                  = Int[Int(key) for key in get_new_interaction_key_range(total_num_interactions, customer_key)],
        interaction_start_ts            = start_times,
        interaction_end_ts              = end_times,
        interaction_channel_type_cd     = channel_type_codes,
        interaction_channel_type_desc   = channel_type_descs,
        interaction_direction           = rep(missing, total_num_interactions),
        interaction_ref_num             = rep(missing, total_num_interactions)
        )
end

"""
Generates the t_Channel_Usage table.

* customer_key A record id number, used as the surrogate key.
* enddate The final date for data generation for the customer asset.
* days The number of days to generate data for.
"""
function create_channel_usage(customer_key::String, enddate::String, days::Int)
    dt = Date(enddate) - Dates.Day(days)
    customer_since_date = get_customer_since_date(customer_key)
    since_date = Date(customer_since_date)
    gomoney_estb_date = Date("2005-01-01")
    ib_estb_date = Date("1995-01-01")
    ib_yn = samp([rep("Y", 8), ["N", "N"]])
    gomoney_yn = samp([rep("Y", 7), rep("N", 3)])
    set_customer_attributes(customer_key, ["gomoney_yn", gomoney_yn, "ib_yn", ib_yn])
    earliest_gm_start_date = since_date > gomoney_estb_date ? customer_since_date : string(gomoney_estb_date)
    earliest_ib_start_date = since_date > ib_estb_date ? customer_since_date : string(ib_estb_date)
    if gomoney_yn == "Y"
        gomoney_registered_date = create_since_date(enddate, startdate=earliest_gm_start_date, min_days=60, min_age_years=0)
    else
        gomoney_registered_date = missing
    end
    if ib_yn == "Y"
        ib_registered_date = create_since_date(enddate, startdate=earliest_ib_start_date, min_days=60, min_age_years=0)
    else
        ib_registered_date = missing
    end
    DataFrame(
        customer_sk                     = rep(parse(Int, customer_key), days),
        channel_usage_dt                = [dt + Dates.Day(i) for i = 0:(days - 1)],
        gomoney_yn                      = rep(gomoney_yn, days),
        gomoney_registered_dt           = rep(gomoney_registered_date, days),
        num_gomoney_logins_last_7_days  = rep(0, days),
        num_gomoney_logins_last_30_days = rep(0, days),
        ib_yn                           = rep(ib_yn, days),
        ib_registered_dt                = rep(ib_registered_date, days),
        num_ib_logins_last_7_days       = rep(0, days),
        num_ib_logins_last_30_days      = rep(0, days)
        )
end

const joint_account_types = ["TXN", "CC", "MT", "SV"]

function create_joint_accounts(customer_accounts::DataFrame)
    @debug("Creating joint accounts")
    customer_keys = String[]
    account_keys = String[]
    account_open_dates = String[]
    account_role_codes = String[]
    account_role_descs = String[]
    for account_type in joint_account_types
        account_keyset = get_diff(string("account_type:", account_type), "used_account_keys")
        n = round(Int, length(account_keyset) * 0.1)
#         if account_type == "CC"
#             append!(account_role_codes, rep("SE", n))
#             append!(account_role_descs, rep("Secondary", n))
#         else
#             append!(account_role_codes, rep("JO", n))
#             append!(account_role_descs, rep("Joint", n))
#         end
        sample_account_keys = sample(account_keyset, n, replace=false)
        add_to_set("used_account_keys", sample_account_keys...)
#         append!(account_keys, sample_account_keys)
        @debug("sample_account_keys", sample_account_keys)
        for account_key in sample_account_keys
            primary_customer_key, account_open_date = get_account_attribute_selection(account_key, ["customer_key", "open_date"])
#             if account_type != "CC"
#                 debug(string("primary_customer_key: ", primary_customer_key))
# #                 sub_customer_accounts = customer_accounts[customer_accounts[:customer_sk] .== primary_customer_key, :]
# #                 println(sub_customer_accounts)
#                 debug(string("account_key: ", account_key))
# #                 println(customer_accounts[customer_accounts[:account_sk] .== account_key, [:customer_account_role_cd, :customer_account_role_desc]])
# #                 sub_customer_accounts[sub_customer_accounts[:account_sk] .== account_key, [:customer_account_role_cd, :customer_account_role_desc]] = DataFrame(customer_account_role_cd="PR", customer_account_role_desc="Primary")
# #                 println(sub_customer_accounts)
# #                 println(customer_accounts[customer_accounts[:customer_sk] .== primary_customer_key, :])
#                 customer_accounts[customer_accounts[:account_sk] .== account_key, [:customer_account_role_cd, :customer_account_role_desc]] = DataFrame(customer_account_role_cd="PR", customer_account_role_desc="Primary")
# #                 println(customer_accounts[customer_accounts[:account_sk] .== account_key, [:customer_account_role_cd, :customer_account_role_desc]])
#             end
            state = get_customer_attribute(string("customer:", primary_customer_key), "state")
#             push!(account_open_dates, account_open_date)
            year = Dates.year(Date(account_open_date))
            matching_keys = get_intersection(string("state:", state), string("year:", year))
            @debug("matching_keys:")
            @debug(matching_keys)
            second_customer_keys = filter(x -> x != primary_customer_key, get_intersection(string("state:", state), string("year:", year)))
            @debug("second_customer_keys:")
            @debug(second_customer_keys)
            if !isempty(second_customer_keys)
                second_customer_key = samp(second_customer_keys)
                push!(customer_keys, second_customer_key)
                push!(account_keys, account_key)
                push!(account_open_dates, account_open_date)
                if account_type == "CC"
                    push!(account_role_codes, "SE")
                    push!(account_role_descs, "Secondary")
                else
                    @debug(string("primary_customer_key: ", primary_customer_key))
                    @debug(string("account_key: ", account_key))
                    @debug(customer_accounts[customer_accounts[!, :account_sk] .== parse(Int, account_key), [:customer_account_role_cd, :customer_account_role_desc]])
                    customer_accounts[customer_accounts[!, :account_sk] .== parse(Int, account_key), [:customer_account_role_cd, :customer_account_role_desc]] = DataFrame(customer_account_role_cd="PR", customer_account_role_desc="Primary")
                    push!(account_role_codes, "JO")
                    push!(account_role_descs, "Joint")
                end
            end
        end
    end

    @debug(string("customer_keys size:", length(customer_keys)))
    @debug(string("account_keys size:", length(account_keys)))
    @debug(string("account_open_dates size:", length(account_open_dates)))
    @debug(string("account_role_codes size:", length(account_role_codes)))
    @debug(string("account_role_descs size:", length(account_role_descs)))

    DataFrame(
        customer_sk                     = Int[parse(Int, key) for key in customer_keys],
        account_sk                      = Int[parse(Int, key) for key in account_keys],
        customer_account_role_start_dt  = account_open_dates,
        customer_account_role_end_dt    = rep("", length(account_keys)), #rep(missing, length(account_keys)),
        customer_account_role_cd        = account_role_codes,
        customer_account_role_desc      = account_role_descs,
        account_held_since_dt           = account_open_dates
        )
end

function write_data(n::Int, enddate::String="2014-10-31", host::String="127.0.0.1", port::Int=6379)
    start_session(host, port)
    persons, mail_addresses, residential_addresses, customers = create_customer_profiles(n, enddate)
    CSV.write("data/persons.csv", persons)
    CSV.write("data/mail_addresses.csv", mail_addresses)
    CSV.write("data/residential_addresses.csv", residential_addresses)
    CSV.write("data/customers.csv", customers)
end

export create_customer_profiles, write_data, create_account_holdings, create_account_details, create_account_balance,
    create_transactions, create_channel_usage, create_interactions,
    create_joint_accounts, rep
end
