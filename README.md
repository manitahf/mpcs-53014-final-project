# MPCS-53014 Final Project: Chicago Taxi & Rideshare Trips

## Overview
This repo contains all scripts and source code for my Chicago Taxi & Rideshare Trip application. The application leverages Chicago Data Portal's Taxi dataset, as well as the Transportation Network (Rideshare) dataset along with weather data for Chicago, IL to provide insight into the pricing of trips in relation to the weather. The taxi trip dataset dates back to 2013 and includes over 140 million trips, while the Rideshare dataset includes over 150 million trips dating back to 2018. As shown below, the key functionality is the ability to access pricing, distance, and duration statistics for a given route and specified weather conditions. A route is defined by a pickup and dropoff Community Area code. (Note: Chicago currently has 77 Community Areas)

![Trip Stats](TripStats.png)

While there is no realtime data feed available for Chicago's Taxi and Transportation Network data, I did implement a web form for user submission of new trips in order to simulate a realtime feed. This realtime data allowed me to develop a speed layer for this application.

![Submit Trip](SubmitTrip.png)

## Running the Application

The web application has been deployed using CodeDeploy to our Load Balanced web servers and can be accessed at the links below:
- [Trip Stats](http://manita-lb-1574432182.us-east-2.elb.amazonaws.com/trip-stats.html)
- [Submit New Trip](http://manita-lb-1574432182.us-east-2.elb.amazonaws.com/submit-trips.html)

The speed layer can be started by running the commands below from the EMR cluster.
```bash
cd /manita/speed-layer-trips/target

spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamTrips uber-speed-layer-trips-1.0-SNAPSHOT.jar b-2.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.198nfg.c7.kafka.us-east-2.amazonaws.com:9092
```
Note that the speed layer must be running in order to use the new trip submission form.

## Design & Implementation Details

### Data Ingestion

### Data Lake & Batch Views


### Serving Layer


### Speed Layer


### Web App
