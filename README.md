# BC-Challenge

This is a take-home challenge that retrieves sample credit data, loads it into a SQLite DB, and contains a REST API to retrieve client data and credit tag stats.

## Files

### requirements.txt

Modules that are not in the default Python package to be installed.

### load_credit_data.py

Using the bzipped file, load the records in a SQLite table called `credit_data`.

The `credit_data` table contains a randomly created UUID, the name, the user's (sample) SSN, and 200 credit tags. Non-negative values contain valid data, negative values indicate error conditions. Data is loaded in batches of 10,000.

### get_random_id.py

After loading the records, get a random UUID from the credit_data table.

### models.py

Contains the database connections and the schema for the `credit_data` table.

### app.py

The main API for retrieving the data. To run it, first install the required modules using the command `pip install -r requirements.txt`. Then run the command `uvicorn app:app --reload` to start the application.

There are 2 endpoints.

#### localhost:8000/user/{user_uuid}

Get all the credit tags from a user, including tags with negative values. Returns JSON. Data returned does _NOT_ include the name and SSN. Invalid UUID's return a 404 error.

#### localhost:8000/tag/{tag}

Given a valid credit tag, calculate the mean, median, and standard deviation. Only positive numbers are included in the calculations. Returns JSON.
