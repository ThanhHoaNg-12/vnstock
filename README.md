
# VNStock_Exp

This project is an experiment with the VNStock library. It gets the information of publicly-listed banks in the Vietnam stock exchange and dumps all of the data to a Postgresql database.

## Environment Variables

To run this project, you will need to add the following environment variables to your .env file. The content of the .env file can be found in .env.template.

`TESTING`: A boolean (True | False). When TESTING=True, the program only gets the data for 2 tickers. TESTING=False means the program will get the data for all 27 publicly-listed banks

`DATABASE_URL`: Postgresql connection url (postgresql://postgres:postgres@localhost:5432/vnstock). Please create a database in Postgresql and include the name of the database in the url.

