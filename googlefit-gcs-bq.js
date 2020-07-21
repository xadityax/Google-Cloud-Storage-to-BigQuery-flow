'use strict';
const fs = require('fs');
const path = require('path');
const http = require('http');
const url = require('url');
const opn = require('open');
const destroyer = require('server-destroy');
const {google} = require('googleapis');
const fitness = google.fitness('v1');
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const {Storage} = require('@google-cloud/storage');
const storage = new Storage();
const now = require('nano-time');
var count=1;
const GCLOUD_PROJECT = 'spiritual-pride-280717';
const GOOGLE_CLOUD_PROJECT = 'spiritual-pride-280717';
const dataTypes = [
    // 'com.google.activity.segment',
    // 'com.google.calories.bmr',
    // 'com.google.calories.expended',
    // 'com.google.cycling.pedaling.cadence',
    // 'com.google.cycling.pedaling.cumulative',
    // 'com.google.heart_minutes',
    // 'com.google.active_minutes',
    // 'com.google.power.sample',
    // 'com.google.step_count.cadence',
    // 'com.google.step_count.delta',
    // 'com.google.activity.exercise',
    // 'com.google.body.fat.percentage',
    // 'com.google.heart_rate.bpm',
    'com.google.height',
    'com.google.weight',
    // 'com.google.cycling.wheel_revolution.rpm',
    // 'com.google.cycling.wheel_revolution.cumulative',
    // 'com.google.distance.delta',
    // 'com.google.location.sample',
    // 'com.google.speed',
    // 'com.google.hydration',
    // 'com.google.nutrition',
    // 'com.google.blood_glucose',
    // 'com.google.blood_pressure',
    // 'com.google.oxygen_saturation',
    // 'com.google.body.temperature',
    // 'com.google.menstruation',
    // 'com.google.vaginal_spotting'
];
const scopes = ['https://www.googleapis.com/auth/fitness.activity.read', 
          'https://www.googleapis.com/auth/fitness.activity.write', 
          'https://www.googleapis.com/auth/fitness.blood_glucose.read',
          'https://www.googleapis.com/auth/fitness.blood_pressure.read',
          'https://www.googleapis.com/auth/fitness.body.read',
          'https://www.googleapis.com/auth/fitness.body_temperature.read',
          'https://www.googleapis.com/auth/fitness.location.read',
          'https://www.googleapis.com/auth/fitness.nutrition.read',
          'https://www.googleapis.com/auth/fitness.oxygen_saturation.read',
          'https://www.googleapis.com/auth/fitness.reproductive_health.read'
          ];

let keys = { "client_id":"",
                    "project_id":"",
                    "auth_uri":"",
                    "token_uri":"",
                    "auth_provider_x509_cert_url":"",
                    "client_secret":"",
                    "redirect_uris":[""],
                    "javascript_origins":[""]}
                    
const oauth2Client = new google.auth.OAuth2(
  keys.client_id,
  keys.client_secret,
  keys.redirect_uris[0]
);
google.options({auth: oauth2Client});

exports.bigQueryStreamer = async event => {

   async function authenticate(scopes) {
      return new Promise((resolve, reject) => {
        try{
            oauth2Client.credentials = {
              access_token: '',
              refresh_token: '',
              scope: 'https://www.googleapis.com/auth/fitness.body.read https://www.googleapis.com/auth/fitness.nutrition.read https://www.googleapis.com/auth/fitness.activity.read https://www.googleapis.com/auth/fitness.blood_pressure.read https://www.googleapis.com/auth/fitness.oxygen_saturation.read https://www.googleapis.com/auth/fitness.reproductive_health.read https://www.googleapis.com/auth/fitness.body_temperature.read https://www.googleapis.com/auth/fitness.blood_glucose.read https://www.googleapis.com/auth/fitness.location.read https://www.googleapis.com/auth/fitness.activity.write',
              token_type: 'Bearer',
              expiry_date: 1594654441066
            }
            resolve(oauth2Client);
        }
        catch(e){
          reject(e);
        }
     })
  }

  async function localStoragetoGCS(bucketName, filename) {
    async function uploadFile() {
      await storage.bucket(bucketName).upload(filename, {
        gzip: true,
        metadata: {
          // Enable long-lived HTTP caching headers Use only if the contents of the file will never change (If the contents will change, use cacheControl: 'no-cache')
          cacheControl: 'no-cache',
        },
      });
      console.log(`${filename} uploaded to ${bucketName}.`);
    }
    uploadFile().catch(console.error);
  }


  async function GCStoBQ(datasetId, tableId,bucketName,filename) {
    async function loadJSONFromGCS() {
      const metadata = {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        schema : {
          fields : [
                    {
                        "name": "minStartTimeNs",
                        "type": "STRING",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "maxEndTimeNs",
                        "type": "STRING",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "dataSourceId",
                        "type": "STRING",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "point",
                        "type": "RECORD",
                        "mode": "REPEATED",
                        "fields": [
                            {
                                "name": "startTimeNanos",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "endTimeNanos",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "dataTypeName",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "originDataSourceId",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "modifiedTimeMillis",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "value",
                                "type": "RECORD",
                                "mode": "REPEATED",
                                "fields" : [
                                  {
                                    "name": "intVal",
                                    "type": "INT64",
                                    "mode": "NULLABLE"
                                  },
                                  {
                                    "name": "mapVal",
                                    "type": "RECORD",
                                    "mode": "REPEATED",
                                    "fields" : [
                                      {
                                        "name": "Key",
                                        "type": "FLOAT64",
                                        "mode": "NULLABLE"
                                      }
                                    ]
                                  }
                                ]
                            }
                        ]
                    }
        ],
        },
        location: 'US',
      };
      const [job] = await bigquery
        .dataset(datasetId)
        .table(tableId)
        .load(storage.bucket(bucketName).file(filename), metadata);
      console.log(`Job ${job.id} completed.`);
      const errors = job.status.errors;
      if (errors && errors.length > 0) {
        throw errors;
      }
    }
    loadJSONFromGCS();
  }


  async function runDataset(client, dname){
    //var cT = now();
    var dataset2 = `1588344866000000-1593615266000000`
    //var dataset = `1585034375000000-${cT}`
    const res = await fitness.users.dataSources.list({
        "userId": "me",
        "dataTypeName": dname
    })
        .then(function (response) {
            response.data.dataSource.forEach(async function (item) {
                console.log(item.dataStreamId);
                const resx = await fitness.users.dataSources.datasets.get({
                    "userId": "me",
                    "dataSourceId": item.dataStreamId,
                    "datasetId": dataset2,
                })
                    .then(async function (response2) {
                        count++;
                        fs.writeFileSync(path.join("/tmp", `${count}${dname}mergedsource.json`), 
                                        JSON.stringify(response2.data));
                        const res2 = await localStoragetoGCS('',`/tmp/${count}${dname}mergedsource.json`)
                                          .then(
                                            function(res2){
                                                  GCStoBQ('','','',
                                                          `${count}${dname}mergedsource.json`);
                                            },
                        function(err){
                          console.error("Execute error", err);
                        });
                    })
            })
        },
            function (err) { console.error("Execute error", err); });
  }

  
   const res2 = await authenticate(scopes)
    .then(function(res2){
        dataTypes.forEach(function(item){
          runDataset(oauth2Client,item);
        }
        )},
   function(err){
    console.error("Execute error", err);
   });
   
  return;

};
