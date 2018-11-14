# Scala
It Contains data ingestion pipeline with kafka for Banking Encrypted data in Base64.

Kafka with Scala

Producer pushes the bulk data in encrypted form and Consumer has task of consuming,converting
the encrypted data from Base64 to UTF8 and storing it in database after doing certain transformations.

Utils

Contains functional and handy codes for usage in another codes.

Manual Adjustment

It is for pushing data from file after creating schema and doing some transformations in Database.
The load is indirect in nature aka the file name that has to be ingested must be searched in a file,
then the data of that file will get pushed.


Update Framework

Framework is designed for updating data in scala since 'UPDATE' keyword is not present till yet.
All we have to do is mutate the update sql in a table and the framework will take the update statement 
from table.
