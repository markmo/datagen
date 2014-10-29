module DataGen

using KeyManager
using StatsBase
using Distributions
using DataFrames
using RedisClient
using Logging

function rep(var, n)
    [var for i = 1:n]
end

function get_since_date(enddate; startdate="1970-01-01", min_days=100, min_age_years=5, n=1)
    st = Date(startdate) + Dates.Year(min_age_years)
    et = Date(enddate) - Dates.Day(min_days)
    dt = int(et - st)
    ev = sort(rand(1:dt, n))
    dates = []
    for x in ev
        dt = st + Dates.Day(x)
        push!(dates, string(dt))
    end
    if n == 1
        dates[1]
    else
        dates
    end
end

function get_domicile_branch(state::String)
    branches = [
        "VIC" => ["Bourke St Mall", "South Yarra", "Toorak", "Northcote", "Moonee Ponds", "Footscray Mall"],
        "NSW" => ["242 Pitt St", "Chifley Square", "Darlinghurst", "Double Bay", "Crows Nest", "Neutral Bay"],
        "QLD" => ["West End", "Fortitude Valley", "Woolloongabba", "Carnidale", "Mount Gravatt Cental", "Wynnum"],
        "WA"  => ["Allendale Square", "Northbridge", "West Perth", "Leederville", "Mount Lawley"],
        "SA"  => ["Hutt St", "Gouger St", "Rundell Mall", "North Adelaide", "Norwood"],
        "ACT" => ["Canberra Centre", "Dickson", "Manuka", "Fyshwick"],
        "NT"  => ["Darwin", "Palmerston", "Winnellie", "Casuarina"]
        ]
    sample(branches[state])
end

function get_credit_behavioural_risk_rating()
    sample([
        sample(550:599, 15),
        sample(600:649, 12),
        sample(650:699, 15),
        sample(700:749, 18),
        sample(750:799, 27),
        sample(800:850, 13)
      ])
end

function get_first_name(gender::String)
    if gender == "M"
        sample(["Bob", "Jim", "Richard", "David"])
    else
        sample(["Alice", "Jane", "Lynley", "Ellen"])
    end
end

function get_last_name()
    sample(["Smith", "Jones", "Wong", "Khan", "Diaz", "Papadakis", "Costa", "Williams", "Nguyen", "Harris"])
end

function get_name_prefix(gender::String)
    if gender == "M"
        "Mr"
    else
        sample(["Ms", "Mrs"])
    end
end

function get_occupation(occupation_cd::String)
    if occupation_cd == "0001"
        "Accountant"
    elseif occupation_cd == "0006"
        "Engineer"
    else
        "Factory Worker"
    end
end

function get_birth_date(enddate)
    age = sample([
        sample(20:24, 96, replace=true),
        sample(25:29, 99, replace=true),
        sample(30:34, 102, replace=true),
        sample(35:39, 105, replace=true),
        sample(40:44, 109, replace=true),
        sample(45:49, 99, replace=true),
        sample(50:54, 94, replace=true),
        sample(55:59, 85, replace=true),
        sample(60:64, 79, replace=true),
        sample(65:69, 62, replace=true),
        sample(70:74, 70, replace=true),
    ])
    ed = Date(enddate) - Dates.Year(age)
    string(ed)
end

function get_employment_status()
    status = sample(vcat(["FT" for i = 1:9], "PT"))
    if status == "FT"
        "Full time"
    else
        "Part time"
    end
end

function get_employer()
    sample(["HR Block", "BHP Bilton", "Rio Tinto", "Telstra", "Wesfarmers", "Woolworths", "Brown Brothers", "Bulla Dairy Foods", "Coca-Cola Amatil", "Foster's Group"])
end

function get_phone(state)
    num = sample(10000000:99999999)
    prefixes = ["VIC" => "03", "NSW" => "02", "QLD" => "07", "ACT" => "06", "SA" => "08", "WA" => "08", "NT" => "08", "TAS" => "03"]
    string(prefixes[state], num)
end

function get_mobile()
    string("04", sample(1000000:9999999))
end

function get_state()
    sample(vcat(
        ["VIC" for i = 1:25],
        ["NSW" for i = 1:32],
        ["QLD" for i = 1:20],
        ["WA"  for i = 1:11],
        ["SA"  for i = 1:7],
        ["ACT" for i = 1:2],
        "NT"))
end

function get_postcode(state::String)
    postcodes = [
        "VIC" => "3121",
        "NSW" => "2089",
        "QLD" => "4014",
        "WA"  => "6025",
        "SA"  => "5035",
        "ACT" => "2600",
        "NT"  => "0801"
        ]
    postcodes[state]
end

function get_street_name()
  sample(["Eskdale", "Queen", "Johnson", "Egan", "Rolleston", "Smith"])
end

function get_street_type()
  sample(["Street", "Road", "Place", "Avenue", "Parade", "Lane", "Crescent", "Close"])
end

function get_person(gender::String, state::String, id, enddate::String)
    occupation_cd = sample(["0001", "0006", "0059"])
    first_name = get_first_name(gender)
    last_name = get_last_name()
    preferred_contact_method = sample(["marketing email","home phone","mobile phone","work phone"])
    home_phone = get_phone(state)
    mobile_phone = get_mobile()
    work_phone = get_phone(state)
    preferred_email = NA
    preferred_phone = NA
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
        customer_sk             = id,
        first_name              = first_name,
        last_name               = last_name,
        preferred_name          = first_name,
        name_prefix             = get_name_prefix(gender),
        name_suffix             = NA,
        occupation_cd           = occupation_cd,
        occupation              = get_occupation(occupation_cd),
        gender_cd               = gender,
        birth_dt                = get_birth_date(enddate),
        deceased_dt             = NA,
        employment_status       = get_employment_status(),
        employer_name           = get_employer(),
        months_with_current_employer = sample(0:200),
        home_phone              = home_phone,
        mobile_phone            = mobile_phone,
        work_phone              = work_phone,
        preferred_contact_method = preferred_contact_method,
        preferred_email         = preferred_email,
        preferred_phone         = preferred_phone,
        preferred_address_type  = sample(["mail", "residential","residential"])
        )
end

function get_address(state::String, id)
    DataFrame(
        customer_sk             = id,
        postcode                = get_postcode(state),
        street_name             = get_street_name(),
        street_type             = get_street_type(),
        state                   = state,
        country                 = "AU",
        dpid                    = NA,
        address_status_cd       = "A",
        address_status_desc     = "Active"
        )
end

function get_customer(person::DataFrame, state::String, enddate::String, id)
    customer_since_date = get_since_date(enddate, startdate=person[1,:birth_dt])
    set_customer_since_date(id, customer_since_date)
    DataFrame(
        customer_sk             = id,
        cap_cis_id              = id,
        customer_type_cd        = "R",
        customer_type_desc      = "Retail",
        customer_status_cd      = "01",
        customer_status_desc    = "Active",
        customer_since_date     = customer_since_date,
        customer_left_dt        = NA,
        customer_bankrupt_dt    = NA,
        customer_domicile_branch_cd = NA,
        customer_domicile_branch_desc = get_domicile_branch(state),
        relationship_manager_cd = NA,
        relationship_manager    = NA,
        rlnshp_manager_assigned_dt = get_since_date(enddate, startdate="2005-01-01", min_days=0, min_age_years=0),
        contact_allowed_yn      = "Y",
        credit_behavioural_risk_rating = get_credit_behavioural_risk_rating(),
        relationship_depth_flag = NA,
        annual_revenue          = 0,
        annual_cost_to_serve    = 0,
        marketing_email         = person[1,:preferred_email],
        statement_email         = person[1,:preferred_email],
        bpay_email              = person[1,:preferred_email]

        )
end

function get_customer_profiles(n=1000, enddate="2014-10-31")
    customer_key_range = get_new_customer_key_range(n)
    mail_addresses = DataFrame()
    residential_addresses = DataFrame()
    persons = DataFrame()
    customers = DataFrame()
    for i in 1:n
        customer_sk = customer_key_range[i]
        gender = sample(["M", "F"])
        state = get_state()
        person = get_person(gender, state, customer_sk, enddate)
        persons = rbind(persons, person)
        mail_address = get_address(state, customer_sk)
        mail_addresses = rbind(mail_addresses, mail_address)
        residential_address = get_address(state, customer_sk)
        residential_addresses = rbind(residential_addresses, residential_address)
        customer = get_customer(person, state, enddate, customer_sk)
        customers = rbind(customers, customer)
    end
    ["persons" => persons, "mail_addresses" => mail_addresses, "residential_addresses" => residential_addresses, "customers" => customers]
end

function get_customer_account_role_cd(account_type_cd)
    if account_type_cd == "CC"
        "PR"
    else
        "SO"
    end
end

function get_customer_account_role_desc(account_type_cd)
    if account_type_cd == "CC"
        "Primary"
    else
        "Sole"
    end
end

function get_account_holdings(customer_sk)
    num_accounts = sample(vcat([1 for i = 1:11], [2 for i = 1:3], [3 for i = 1:6], [4 for i = 1:6], [5 for i = 1:6]))
    if num_accounts == 1
        account_type_codes = [sample(vcat(["CC" for i = 1:73], ["TXN" for i = 1:9], ["PL" for i = 1:9], ["TD" for i = 1:9]))]
    elseif num_accounts == 2
        account_type_codes = vcat("TXN", sample(["TD", "TD", "PL"]))
    elseif num_accounts == 3
        account_type_codes = vcat(["TXN", "CC"], sample(["SV", "SV", "MT"]))
    elseif num_accounts == 4
        account_type_codes = vcat(["TXN", "CC", "MT"], sample(["SV", "MT", "CC"]))
    elseif num_accounts == 5
        account_type_codes = vcat(["TXN", "CC", "MT", "SV"], sample(vcat("TD", rep("MT", 3))))
    end
    customer_account_role_codes = [get_customer_account_role_cd(account_type_codes[i]) for i in 1:num_accounts]
    customer_account_role_descs = [get_customer_account_role_desc(account_type_codes[i]) for i in 1:num_accounts]
    account_keys = get_new_account_keys(customer_sk, account_type_codes)
    customer_since_date = get_customer_since_date(customer_sk)
    set_account_open_dates(account_keys, [customer_since_date for i = 1:num_accounts])
    DataFrame(
        customer_sk             = [customer_sk for i = 1:num_accounts],
        account_sk              = account_keys,
        customer_account_role_start_dt = [customer_since_date for i = 1:num_accounts],
        customer_account_role_end_dt = [NA for i = 1:num_accounts],
        customer_account_role_cd = customer_account_role_codes,
        customer_account_role_desc = customer_account_role_descs,
        account_held_since_dt   = [customer_since_date for i = 1:num_accounts]
        )
end

function get_account_type_descs(account_type_codes)
    account_types = [
        "MT" => "Mortgage",
        "CC" => "Credit Card",
        "TD" => "Term Deposit",
        "PL" => "Personal Loan",
        "TXN" => "Transaction Account",
        "SV" => "Savings Account"
        ]
    [get!(account_types, code, "Not set") for code in account_type_codes]
end

function get_num_transactions(account_type_code::String, days::Int64)
    if account_type_code in ["CC", "TXN"]
        sample(1:5, days, replace=true)
    elseif account_type_code in ["MT", "PL"]
        sample(vcat(rep(0, days - 1), 1), days)
    elseif account_type_code == "SV"
        sample([1, 0, 0], days, replace=true)
    elseif account_type_code == "TD"
        sample(vcat(1, rep(0, 50)), days, replace=true)
    end
end

function get_sign(account_type_code::String)
    if account_type_code in ["MT", "PL"]
        "DR"
    elseif account_type_code == "TD"
        sample(["CR", "CR", "DR"])
    elseif account_type_code in ["CC", "TXN"]
        sample(vcat("CR", rep("DR", 28)))
    elseif account_type_code == "SV"
        sample(vcat(rep("CR", 20), "DR"))
    end
end

function get_amount(account_type_code::String, transaction_dr_cr::String)
    if account_type_code == "MT"
        sample(-400000:-100000) / 100
    elseif account_type_code == "TD" && transaction_dr_cr == "DR"
        sample([-1000, -2000, -3000, -5000, -7500])
    elseif account_type_code == "TD" && transaction_dr_cr == "CR"
        sample([1000, 2000, 3000, 5000, 7500, sample(8000:30000) / 100])
    elseif account_type_code == "TXN" && transaction_dr_cr == "DR"
        sample(vcat([-round(rand(Normal(10., 3.)),2) for i = 1:5], -round(rand(Normal(110., 40.)),2)))
    elseif account_type_code == "TXN" && transaction_dr_cr == "CR"
        sample([1100, 2200, 3300, 4400, 5500, 6600, 7700, 8800, 9900])
    elseif account_type_code == "CC" && transaction_dr_cr == "DR"
        sample(100000:300000) / 100
    elseif account_type_code == "CC" && transaction_dr_cr == "CR"
        sample(vcat([-round(rand(Normal(20., 6.)),2) for i = 1:5], -round(rand(Normal(200., 40.)),2)))
    elseif account_type_code == "SV"
        sample(50:250)
    elseif account_type_code == "PL"
        sample(-25000:5000) / 100
    end
end

function get_opening_balance(account_type_code::String)
    if account_type_code == "MT"
        sample(10000000:90000000) / 100
    elseif account_type_code == "TD"
        sample(500000:50000000) / 100
    elseif account_type_code == "TXN"
        sample(500000:2000000) / 100
    elseif account_type_code == "CC"
        sample(200000:1500000) / 100
    elseif account_type_code == "SV"
        sample(500000:20000000) / 100
    elseif account_type_code == "PL"
        sample(500000:8000000) / 100
    end
end

function get_account_details(customer_sk, enddate::String)
    core_customer_details = get_customer_accounts(customer_sk)
    DataFrame(
        account_sk              = core_customer_details[:account_key],
        account_id              = core_customer_details[:account_key],
        account_type_cd         = core_customer_details[:account_type],
        account_type_desc       = get_account_type_descs(core_customer_details[:account_type]),
        account_open_dt         = core_customer_details[:open_date],
        account_close_dt        = NA,
        account_status_cd       = "A",
        account_status_desc     = "Active"
        )
end

function get_account_balance(account_sk, enddate, days)
    dt = Date(enddate) - Dates.Day(days)
    account_type_code = get_account_type(account_sk)
    opening_balance = get_opening_balance(account_type_code)
    DataFrame(
        account_sk              = rep(account_sk, days),
        account_balance_type_cd = rep(NA, days),
        account_balance_type_desc = rep(NA, days),
        account_balance_dt      = [dt + Dates.Day(i) for i = 0:(days - 1)],
        account_balance_val     = vcat(opening_balance, rep(0, days - 1))
        )
end

function get_transactions(account_sk, enddate, days)
    account_type_code = get_account_type(account_sk)
    transactions_per_day = get_num_transactions(account_type_code, days)
    num_transactions = sum(transactions_per_day)
    times = []
    amounts = []
    dt = Date(enddate) - Dates.Day(days + 1)
    for i in 1:days
        dt = dt + Dates.Day(1)
        if transactions_per_day[i] > 0
            for j in 1:transactions_per_day[i]
                ts = string(dt)
                sign = get_sign(account_type_code)
                amount = get_amount(account_type_code, sign)
                push!(times, ts)
                push!(amounts, amount)
            end
        end
    end
    if num_transactions > 0
        DataFrame(
            transaction_sk                = get_new_transaction_key_range(num_transactions),
            transaction_ts                = times,
            transaction_amount            = amounts,
            transaction_channel_type_cd   = rep(NA, num_transactions),
            transaction_channel_type_desc = rep(NA, num_transactions),
            transaction_dr_cr             = rep(NA, num_transactions),
            transaction_ref_num           = rep(NA, num_transactions),
            transaction_status_cd         = rep(NA, num_transactions),
            transaction_status_desc       = rep(NA, num_transactions),
            merchant_id                   = rep(NA, num_transactions),
            account_sk                    = rep(account_sk, num_transactions)
            )
    else
        nothing
    end
end

function get_channel_type_cd(ib_yn, gomoney_yn, num_interactions)
    if ib_yn == "Y" && gomoney_yn == "Y"
        sample(["GM", "IB"], num_interactions, replace=true)
    elseif ib_yn == "Y" && gomoney_yn == "N"
        ["IB" for i = 1:num_interactions]
    elseif ib_yn == "N" && gomoney_yn == "Y"
        ["GM" for i = 1:num_interactions]
    end
end

function get_channel_type_desc(channel_type_codes::Array)
    [code == "IB" ? "Internet Banking" : "GoMoney" for code in channel_type_codes]
end

function get_num_interactions(days)
    sample([0, 0, 1, 2], days, replace=true)
end

function get_interactions(ib_yn, gomoney_yn, num_interactions)
    interactions_per_day = get_num_interactions(days)
    total_num_interactions = sum(interactions_per_day)
    channel_type_cd = get_channel_type_cd(ib_yn, gomoney_yn, total_num_interactions)
    channel_type_desc = get_channel_type_desc(channel_type_cd)
    start_times = []
    end_times = []
    start_ts = DateTime(enddate) - Dates.Day(days + 1)
    for i in 1:days
        start_ts = start_ts + Dates.Day(1)
        if interactions_per_day > 0
            end_ts = start_ts + Dates.Day(sample(10:180))
            push!(start_times, start_ts)
            push!(end_times, end_ts)
        end
    end
    DataFrame(
        interaction_sk          = get_new_interaction_key_range(total_num_interactions, customer_key),
        interaction_start_ts    = start_times,
        interaction_end_ts      = end_times,
        interaction_channel_type_cd = channel_type_cd,
        interaction_channel_type_desc = channel_type_desc,
        interaction_direction   = rep(NA, total_num_interactions),
        interaction_ref_num     = rep(NA, total_num_interactions)
        )
end

function get_channel_usage(customer_sk, enddate, days)
    dt = Date(enddate) - Dates.Day(days)
    customer_since_dt = get_customer_since_date(customer_sk)
    since_dt = Date(customer_since_dt)
    gomoney_estb_dt = Date("2005-01-01")
    ib_estb_dt = Date("1995-01-01")
    ib_yn = sample(vcat(["Y" for i = 1:8], ["N", "N"]))
    set_customer_attribute(customer_sk, "ib_yn", ib_yn)
    gomoney_yn = sample(vcat(["Y" for i = 1:7], ["N" for i = 1:3]))
    set_customer_attribute(customer_sk, "gomoney_yn", gomoney_yn)
    earliest_gm_start_dt = since_dt > gomoney_estb_dt ? customer_since_dt : gomoney_estb_dt
    earliest_ib_start_dt = since_dt > ib_estb_dt ? customer_since_dt : ib_estb_dt
    if gomoney_yn == "Y"
        gomoney_registered_dt = get_since_date(enddate, startdate=earliest_gm_start_dt, min_days=60, min_age_years=0)
    else
        gomoney_registered_dt = NA
    end
    if ib_yn == "Y"
        ib_registered_dt = get_since_date(enddate, startdate=earliest_ib_start_dt, min_days=60, min_age_years=0)
    else
        ib_registered_dt = NA
    end
    DataFrame(
        customer_sk             = rep(customer_sk, days),
        channel_usage_dt        = [dt + Dates.Day(i) for i = 0:(days - 1)],
        gomoney_yn              = rep(gomoney_yn, days),
        gomoney_registered_dt   = rep(gomoney_registered_dt, days),
        num_gomoney_logins_last_7_days = rep(0, days),
        num_gomoney_logins_last_30_days = rep(0, days),
        ib_yn                   = rep(ib_yn, days),
        ib_registered_dt        = rep(ib_registered_dt, days),
        num_ib_logins_last_7_days = rep(0, days),
        num_ib_logins_last_30_days = rep(0, days)
        )
end

function write_data(n, enddate="2014-10-31", host="127.0.0.1", port=6379)
    start_session(host, port)
    profiles = get_customer_profiles(n, enddate)
    writetable("data/persons.csv", profiles["persons"])
    writetable("data/mail_addresses.csv", profiles["mail_addresses"])
    writetable("data/residential_addresses.csv", profiles["residential_addresses"])
    writetable("data/customers.csv", profiles["customers"])
end

export get_customer_profiles, write_data, get_account_holdings, get_account_details, get_account_balance,
    get_transactions, get_channel_usage, get_interactions, get_customer_interaction_keys

end
