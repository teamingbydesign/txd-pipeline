Here's the instructions for using our MongoDB from Mihir who worked on setting up the backend last semester. 

TXD Research (URAP Spring 2024) By Mihir Mirchandanimongosh \
proper login \
`mongosh "mongodb+srv://teamdb.r3grvrf.mongodb.net/" --apiVersion 1 --username <username>` \
REPLACE <username> with username

To add new CSV Files to the database: \
`mongosh` \
`use teamdb` \
import the CSV file: \
`mongoimport --db teamdb --collection <NEW NAME> --type csv --file "<PATH TO CSV>" --headerline`

query the new collection: \
`db.teamData.find().pretty()`


To add new CSV to MongoDB Atlas \
`mongoimport --uri "mongodb+srv://<username>:<passwd>@teamdb.r3grvrf.mongodb.net/?retryWrites=true&w=majority&appName=teamdb" --db teamdb --collection teamData --type csv --file "C:\Users\mihir\git\research\csv_data\ME292C_CHECKIN1_MACROANALYSIS_v1_modified.csv" --headerline`

To query all the collections in the db (verify what CSVs exist) \
`mongosh` \
`use teamdb` \
`show collections` 

To delete a collection \
`mongosh` \
`use teamdb;` \
`db.taskData.drop();` \

Username: root \
Password: root