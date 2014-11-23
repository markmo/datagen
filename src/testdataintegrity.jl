srcdir = dirname(@__FILE__)

set = Set()
f = open(srcdir * "/../data/t_Customer_Account_20141031.csv", "r")
for line in eachline(f)
    cols = split(line, ',')
    key = cols[2] * ":" * cols[3] * ":" * cols[4]
    if in(key, set)
        error("Duplicate key: " * key * " (line: " * cols[1] * ")")
    else
        push!(set, key)
    end
    print(".")
end
close(f)

set = Set()
f = open(srcdir * "/../data/t_Customer_Interaction_20141030.csv", "r")
for line in eachline(f)
    cols = split(line, ',')
    key = cols[2] * ":" * cols[3]
    if in(key, set)
        error("Duplicate key: " * key * " (line: " * cols[1] * ")")
    else
        push!(set, key)
    end
    print(".")
end
close(f)

set = Set()
f = open(srcdir * "/../data/t_Account_Balance_20141031.csv", "r")
for line in eachline(f)
    cols = split(line, ',')
    key = cols[2] * ":" * cols[5]
    if in(key, set)
        error("Duplicate key: " * key * " (line: " * cols[1] * ")")
    else
        push!(set, key)
    end
    print(".")
end
close(f)

set = Set()
f = open(srcdir * "/../data/t_Channel_Usage_20141030.csv", "r")
for line in eachline(f)
    cols = split(line, ',')
    key = cols[2] * ":" * cols[3]
    if in(key, set)
        error("Duplicate key: " * key * " (line: " * cols[1] * ")")
    else
        push!(set, key)
    end
    print(".")
end
close(f)
