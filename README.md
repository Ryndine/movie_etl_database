# Movie ETL & Database

## Objective: 
Clean wikipedia and kaggle data in order to create a database for analysis.

## Tools & databases used:
- Python
- Jupyter Notebook
- SQLAlchemy
- PostgreSQL

## Preparing Analysis:

**Cleaning wikipedia data:**

- First step I quickly analyze the data by checking the columns names to see what data I have.  
	- Immediately I know I want to filter out data to make sure all movies contain a Director, IMBD link, and is not a TV show.

- Next I notice that the data contains names of movies in different languages, so I want to condense this into it's own column called "alt_titles".
	- To do this I make a function that will iterate through the columns, pop out the alternate language data and combine everything into a dictionary.
	- After that I'm cleaning up column names.

- The data contains duplicate rows, so in order to clean these I'm using regex to extract the IMDB identifier from the IMBD link. Then dropping where there are duplicates.
	- From here I clean out any remaining null values from the dataframe.

- Box office data needed to be converted and parsed.
	- To do this I'm converting all lists to stings
	- Next I'm creating two regex variables to catch: "$123.4 million" (or billion) and "$123,456,789"
	- Using a function I'm cleaning the values by removing dollar signs, commas, million & billion text, and converting the values to floats and their proper numeric values.
	
- Budget data needs the same cleanup.
	- First I'm removing all values that are between doller signs and hyphens.
	- Next I'm reusing the regex from box office to catch what needs to be converted.
	- Citation references needed to be removed before moving forward.
	- Lastly I'm reusing the method from box office to convert budget data to proper values.

- Release dates and run time cleanup.	
	- To cleanup the dates I'm using regex to convert data into YEAR-MONTH-DAY format.
	- To cleanup the runtimes I'm using regex to convert the numeric-string format to numeric.

**Cleaning kaggle data:**

- After inspecting the data I notice there's an adult column.
	- Quick cleanup by finding all data that is tagged "adult" then dropping it.

- For video data, all I need to know is if there is a video so I'm converting the data to boolean.

- The columns: budget, id, and popularity are being converted to numeric.

- Release data column converted to datetime.

- The ratings.csv kaggle file just needs to be converted to a datetime (in seconds)

** Inspecting data:**

- Before inspecting the data I'm merging wikipedia data and the kaggle data together

- I'm generating a quick plot and statistical analysis for the ratings data to see check for issues.

- Next I'm checking the wiki data to the kaggle data.
	- Title names contain differences
	- I'm using scatter plots to check numeric values between the two datasets.
	- After noticing an outlier in release data I inspected it further and cleaned it up.
	- I'm comparing movie languages with simple value_counts, wiki data contains nulls.
	- Last I'm checking production companies, the wikidata is less informative.
	
- After inspecting the data I've decided to remove Title, Release Data, Language, and Production data provided by wikipedia.

- I'm using a function in order to fill missing data for columns then drop redundant columns.
	- I'm using this function on the numeric columns: Runtime, Budget, and Revenue

- Afterwards I'm checking if any columns contain one value

- For merging rata data I'm first converting some data in order to see more numeric information.
	- I'm doing a groupby on movieId and rating, then getting the count to display how many users rated the a given movie per rating group.
	- Doing a pivot on that dataframe to make sure movieId was indexed.
	- Adding "rating_" to the newly ccreated rating columns to see rating groups.
	- Then merging the newly created data on the kaggle_id column.
	
**Database:**

- After doing extracting and transforming the data, I'm ready to load it into a PostgreSQL database!