'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({host: process.argv[3], port: Number(process.argv[4]), encoding: 'latin1'})

function rowToMap(row) {
    var stats = {}
    if (row != null){
        row.forEach(function (item) {
            stats[item['column']] = Number(Buffer.from(item['$'], 'latin1').readBigInt64BE())
            if(item['column'].includes('price') || item['column'].includes('distance')){
                stats[item['column']] = stats[item['column']] / 100.0
            }
        });
    }
    return stats;
}

function getTemperatureType(temperature) {
    if(temperature < 15) { return ['0', 'Very cold'];}
    if(temperature < 39) { return ['1', 'Cold'];}
    if(temperature < 59) { return ['2', 'Moderate'];}
    if(temperature < 79) { return ['3', 'Warm'];}
    else { return ['4', 'Hot'];}
}

function getWeatherString(weather) {
    if(weather === "clear") {return "clear conditions";}
    if(weather === "thunder") {return "thunderstorms";}
    else {return weather;}
}

app.use(express.static('public'));
app.get('/get-stats.html', function (req, res) {
    const pickup = req.query['pickup'];
    const dropoff = req.query['dropoff'];
    const temperature = req.query['temperature'];
    const weather = req.query['weather'];

    hclient.table('manita_trip_stats_by_route_and_weather').row(pickup + dropoff + weather + getTemperatureType(temperature)[0]).get(function (err, cells) {
        const routeInfo = rowToMap(cells);
        var rideshare_trips = routeInfo["rideshare:trips"] || 0;
        var rideshare_total_price = (routeInfo["rideshare:total_price"] || 0).toFixed(2);
        var rideshare_avg_price = rideshare_trips === 0 ? " - " : (rideshare_total_price / rideshare_trips).toFixed(2);

        var taxi_trips = routeInfo["taxi:trips"] || 0;
        var taxi_total_price = (routeInfo["taxi:total_price"] || 0).toFixed(2);
        var taxi_avg_price = taxi_trips === 0 ? " - " : (taxi_total_price / taxi_trips).toFixed(2);

        var total_trips = rideshare_trips + taxi_trips;
        var avg_duration = total_trips === 0 ? " - " : ((routeInfo["rideshare:total_duration"] + routeInfo["taxi:total_duration"]) / (60 * total_trips)).toFixed(0);
        var avg_distance = total_trips === 0 ? " - " : ((routeInfo["rideshare:total_distance"] + routeInfo["taxi:total_distance"]) / total_trips).toFixed(1);

        var template = filesystem.readFileSync("result.mustache").toString();
        var html = mustache.render(template, {
            pickup: req.query['pickup'],
            dropoff: req.query['dropoff'],
            weather: getWeatherString(req.query['weather']),
            temperature: getTemperatureType(temperature)[1],
            rideshare_trips: rideshare_trips.toLocaleString(),
            taxi_trips: taxi_trips.toLocaleString(),
            rideshare_min_price: (routeInfo["rideshare:min_price"] || 0).toFixed(2),
            taxi_min_price: (routeInfo["taxi:min_price"] || 0).toFixed(2),
            rideshare_max_price: (routeInfo["rideshare:max_price"] || 0).toFixed(2),
            taxi_max_price: (routeInfo["taxi:max_price"] || 0).toFixed(2),
            rideshare_avg_price: rideshare_avg_price,
            taxi_avg_price: taxi_avg_price,
            avg_distance: avg_distance,
            avg_duration: avg_duration
        });
        res.send(html);
    });
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/submit.html',function (req, res) {
    var trip = {
        trip_date : new Date().toString(),
        pickup_area : req.query['pickup'],
        dropoff_area : req.query['dropoff'],
        trip_total : Math.round(req.query['total']*100),
        trip_miles : Math.round(req.query['miles']*100),
        trip_seconds : Math.round(req.query['duration']*60),
        trip_type: req.query['trip_type']
    };

    kafkaProducer.send([{ topic: 'manita_trips', messages: JSON.stringify(trip)}],
        function (err, data) {
            console.log(JSON.stringify(trip));
            res.redirect('submit-trips.html');
        });
});

app.listen(port);
