import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix


# Make plots look better
sns.set(style="whitegrid")


# ✅ Connect to MySQL database
engine = create_engine("mysql+pymysql://root:R%40hul123@localhost:3306/dwh_airport_db")

# ✅ Load dimension and fact tables
fact_flight = pd.read_sql("SELECT * FROM fact_flight", engine)
dim_airport = pd.read_sql("SELECT * FROM dim_airport", engine)
dim_airline = pd.read_sql("SELECT * FROM dim_airline", engine)

#  Join fact_flight with source (from) airport
flight_df = fact_flight.merge(dim_airport.add_prefix("from_"),
                              left_on="from_airport_id", right_on="from_airport_id", how="left")

#  Join with destination (to) airport
flight_df = flight_df.merge(dim_airport.add_prefix("to_"),
                            left_on="to_airport_id", right_on="to_airport_id", how="left")

#  Join with airline
flight_df = flight_df.merge(dim_airline, left_on="airline_id", right_on="airline_key", how="left")

#  Convert date fields
flight_df["arrival"] = pd.to_datetime(flight_df["arrival"])
flight_df["departure"] = pd.to_datetime(flight_df["departure"])

#  Convert TIME field (schedule_arrival) to timedelta
flight_df["schedule_arrival"] = pd.to_timedelta(flight_df["schedule_arrival"].astype(str))

#  Construct full schedule arrival datetime using departure date + schedule arrival time
flight_df["schedule_arrival_full"] = flight_df["departure"].dt.normalize() + flight_df["schedule_arrival"]

#  Delay in minutes
flight_df["delay_minutes"] = (flight_df["arrival"] - flight_df["schedule_arrival_full"]).dt.total_seconds() / 60
flight_df["is_delayed"] = (flight_df["delay_minutes"] > 15).astype(int)

#  Feature Engineering
flight_df["departure_hour"] = flight_df["departure"].dt.hour          # Hour of day
flight_df["weekday"] = flight_df["departure"].dt.weekday              # 0=Monday
flight_df["duration_minutes"] = (flight_df["arrival"] - flight_df["departure"]).dt.total_seconds() / 60
flight_df["route"] = flight_df["from_city"] + "-" + flight_df["to_city"]

#  Final preview
print(flight_df.head())
'''
# distribution of delay minutes
plt.figure(figsize=(10, 5))
sns.histplot(flight_df["delay_minutes"], bins=50, kde=True)
plt.title("Distribution of Flight Delays (in minutes)")
plt.xlabel("Delay Minutes")
plt.ylabel("Number of Flights")
plt.xlim(-100, 300)
#plt.show()

#top 10 most delayed routes
route_delay = flight_df.groupby("route")["delay_minutes"].mean().sort_values(ascending=False).head(10)

plt.figure(figsize=(12, 6))
sns.barplot(x=route_delay.values, y=route_delay.index)
plt.title("Top 10 Routes by Average Delay")
plt.xlabel("Average Delay (minutes)")
plt.ylabel("Route")
#plt.show()

#average delay by weekend
weekday_delay = flight_df.groupby("weekday")["delay_minutes"].mean()

plt.figure(figsize=(8, 5))
sns.barplot(x=weekday_delay.index, y=weekday_delay.values)
plt.title("Average Delay by Day of Week")
plt.xlabel("Weekday (0 = Monday)")
plt.ylabel("Average Delay (minutes)")
#plt.show()

#average delay per airline

airline_delay = flight_df.groupby("airlinename")["delay_minutes"].mean().sort_values(ascending=False)

plt.figure(figsize=(12, 6))
sns.barplot(x=airline_delay.values, y=airline_delay.index)
plt.title("Average Delay by Airline")
plt.xlabel("Average Delay (minutes)")
plt.ylabel("Airline")
#plt.show()

#delay by departure hour
hourly_delay = flight_df.groupby("departure_hour")["delay_minutes"].mean()

plt.figure(figsize=(10, 5))
sns.lineplot(x=hourly_delay.index, y=hourly_delay.values, marker='o')
plt.title("Average Delay by Hour of Departure")
plt.xlabel("Hour of Day")
plt.ylabel("Average Delay (minutes)")
plt.grid(True)
#plt.show()
'''

features = [
    "departure_hour",
    "weekday",
    "duration_minutes",
    #"airline_id",
    "from_airport_id",
    "to_airport_id"
]
target = "is_delayed"
# Select features and target
X = flight_df[features]
y = flight_df[target]

# Fill missing or invalid values if any
X = X.fillna(0)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)

print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nClassification Report:\n", classification_report(y_test, y_pred))
print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))


