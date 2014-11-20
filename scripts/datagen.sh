#!/usr/bin/env bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

julia -p 4 $scriptdir/../src/run.jl > /var/log/datagen/datagen.log

numblocksfile="$scriptdir/../numblocks"
numblocks=$(cat $numblocksfile)

echo "numblocks=$numblocks"

catpersoncmd="cat $scriptdir/../data/persons_header.csv"
catmailaddrcmd="cat $scriptdir/../data/mail_addresses_header.csv"
catresaddrcmd="cat $scriptdir/../data/residential_addresses_header.csv"
catcustcmd="cat $scriptdir/../data/customers_header.csv"
catacctbalcmd="cat $scriptdir/../data/account_balances_header.csv"
cataccttranscmd="cat $scriptdir/../data/account_transactions_header.csv"
catchanusagecmd="cat $scriptdir/../data/channel_usage_header.csv"
catcustacctcmd="cat $scriptdir/../data/customer_accounts_header.csv"
catacctcmd="cat $scriptdir/../data/accounts_header.csv"
catintercmd="cat $scriptdir/../data/interactions_header.csv"
catcustintercmd="cat $scriptdir/../data/customer_interactions_header.csv"

for i in `seq 1 $numblocks`;
do
	catpersoncmd="$catpersoncmd $scriptdir/../data/persons_$i.csv"
	catmailaddrcmd="$catmailaddrcmd $scriptdir/../data/mail_addresses_$i.csv"
	catresaddrcmd="$catresaddrcmd $scriptdir/../data/residential_addresses_$i.csv"
	catcustcmd="$catcustcmd $scriptdir/../data/customers_$i.csv"
	catacctbalcmd="$catacctbalcmd $scriptdir/../data/account_balances_$i.csv"
	cataccttranscmd="$cataccttranscmd $scriptdir/../data/account_transactions_$i.csv"
	catchanusagecmd="$catchanusagecmd $scriptdir/../data/channel_usage_$i.csv"
	catcustacctcmd="$catcustacctcmd $scriptdir/../data/customer_accounts_$i.csv"
	catacctcmd="$catacctcmd $scriptdir/../data/accounts_$i.csv"
	catintercmd="$catintercmd $scriptdir/../data/interactions_$i.csv"
	catcustintercmd="$catcustintercmd $scriptdir/../data/customer_interactions_$i.csv"
done;
catpersoncmd="$catpersoncmd > $scriptdir/../data/t_Person_20141030.csv"
catmailaddrcmd="$catmailaddrcmd > $scriptdir/../data/t_Mail_Contact_20141030.csv"
catresaddrcmd="$catresaddrcmd > $scriptdir/../data/t_Residential_Contact_20141030.csv"
catcustcmd="$catcustcmd > $scriptdir/../data/t_Customer_20141030.csv"
catacctbalcmd="$catacctbalcmd > $scriptdir/../data/t_Account_Balance_20141031.csv"
cataccttranscmd="$cataccttranscmd > $scriptdir/../data/t_Account_Transaction_20141031.csv"
catchanusagecmd="$catchanusagecmd > $scriptdir/../data/t_Channel_Usage_20141030.csv"
catcustacctcmd="$catcustacctcmd > $scriptdir/../data/t_Customer_Account_20141031.csv"
catacctcmd="$catacctcmd > $scriptdir/../data/t_Account_20141031.csv"
catintercmd="$catintercmd > $scriptdir/../data/t_Interaction_20141030.csv"
catcustintercmd="$catcustintercmd > $scriptdir/../data/t_Customer_Interaction_20141030.csv"

echo "Writing data/t_Person_20141030.csv"
eval $catpersoncmd
echo "Writing data/t_Mail_Contact_20141030.csv"
eval $catmailaddrcmd
echo "Writing data/t_Residential_Contact_20141030.csv"
eval $catresaddrcmd
echo "Writing data/t_Customer_20141030.csv"
eval $catcustcmd
echo "Writing data/t_Account_Balance_20141031.csv"
eval $catacctbalcmd
echo "Writing data/t_Account_Transaction_20141031.csv"
eval $cataccttranscmd
echo "Writing data/t_Channel_Usage_20141030.csv"
eval $catchanusagecmd
echo "Writing data/t_Customer_Account_20141031.csv"
eval $catcustacctcmd
echo "Writing data/t_Account_20141031.csv"
eval $catacctcmd
echo "Writing data/t_Interaction_20141030.csv"
eval $catintercmd
echo "Writing data/t_Customer_Interaction_20141030.csv"
eval $catcustintercmd

julia $scriptdir/../src/postprocess.jl

$scriptdir/move_data.sh
