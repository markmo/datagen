#!/usr/bin/env bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
numblocksfile="$scriptdir/../numblocks"
numblocks=$(cat $numblocksfile)

echo "numblocks=$numblocks"

catpersoncmd="cat "
catmailaddrcmd="cat "
catresaddrcmd="cat "
catcustcmd="cat "
catacctbalcmd="cat "
cataccttranscmd="cat "
catchanusagecmd="cat "
catcustacctcmd="cat "
catacctcmd="cat "
catintercmd="cat "
catcustintercmd="cat "
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
