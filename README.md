## Data Modeling with Postgres

### Purpose of the project
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Results achieved

Data was extracted from multiples song and log data sources ,then collected, cleaned up and transformed the data in Postgres Database. The data was modeled based on star schema.

Steps followed:
1 - Song_Data and log_data were extracted using Pandas API.
2 - Song_Data was transformed into Songs and Artists dimension tables.
3 - Log data was transformed into Time and Users dimension table which involved multiple datetime based transformations.
4 - Later the data from 4 tables were modeled to form a single fact table called song_plays

### Example queries and Results

##### SELECT * FROM songplays LIMIT 1;

songplay_id	start_time	user_id	level	song_id	artist_id	session_id	location	user_agent
0	2018-11-30 00:22:07.796000	91	free	None	None	829	Dallas-Fort Worth-Arlington, TX	Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)

##### SELECT * FROM songplays where song_id is not null limit 5;

songplay_id	start_time	user_id	level	song_id	artist_id	session_id	location	user_agent
4677	2018-11-21 21:56:47.796000	15	paid	SOZCTXZ12AB0182364	AR5KOSW1187FB35FF4	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"