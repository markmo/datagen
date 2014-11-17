srcdir = dirname(@__FILE__)

files = String[
    "t_Person_20141030.csv",
    "t_Mail_Contact_20141030.csv",
    "t_Residential_Contact_20141030.csv",
    "t_Customer_20141030.csv",
    "t_Account_Balance_20141031.csv",
    "t_Account_Transaction_20141031.csv",
    "t_Channel_Usage_20141030.csv",
    "t_Customer_Account_20141031.csv",
    "t_Account_20141031.csv",
    "t_Interaction_20141030.csv",
    "t_Customer_Interaction_20141030.csv"
    ]

for file in files
    filepath = string(srcdir, "/../data/", file)
    tempfilepath = string(filepath, ".temp")
    tempfile = open(tempfilepath, "w")
    firstline = true
    open(filepath, "r") do f
        for (i, line) in enumerate(eachline(f))
            if firstline
                print(tempfile, string("\"\",", line))
                firstline = false
            else
                print(tempfile, string(i-1, ",", line))
            end
        end
    end
    close(tempfile)
    run(`mv $tempfilepath $filepath`)
end