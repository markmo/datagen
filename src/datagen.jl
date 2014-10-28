module DataGen

using KeyManager
using StatsBase

function get_since_date(enddate, startdate="1970-01-01", min_days=60, min_age_years=5, n=1)
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
