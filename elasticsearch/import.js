//const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
    const client = new Client({ node: ELASTIC_SEARCH_URI });

    // Drop index if exists
    await client.indices.delete({
        index: INDEX_NAME,
        ignore_unavailable: true
    });

    await client.indices.create({
        index: INDEX_NAME,
        body: {
            // TODO configurer l'index https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
            mappings: {
                properties: {
                    location: {
                        "type": "geo_point"
                    },
                    timeStamp: { "type": "date" }
                }
            }
        }
    });

    const calls = [];
    fs.createReadStream('../911.csv')
        .pipe(csv())
        .on('data', data => {
            // TODO créer l'objet call à partir de la ligne
            const call = {
                'location': { 'lat': parseFloat(data.lat), 'lon': parseFloat(data.lng) },
                'desc': data.desc,
                'zip': data.zip,
                'category': data.title.split(":")[0],
                'type': data.title.split(": ")[1],
                'timeStamp': data.timeStamp.split(" ")[0].trim(),
                'twp': data.twp,
                'addr': data.addr
            };
            calls.push(call)
        })
        .on('end', async() => {
            console.log(calls[0])
            const body = calls.reduce((calls, call) => {
                calls.push({ index: { _index: '911-calls', _type: '_doc' } })
                calls.push(call)
                return calls
            }, []);
            // TODO insérer les données dans ES en utilisant l'API de bulk https://www.elastic.co/guide/en/elasticsearch/reference/7.x/docs-bulk.html
            client.bulk({ refresh: true, body }, (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} ` + JSON.stringify(resp.body.items[0]));
                client.close();
            });
        });
}

run().catch(console.log);