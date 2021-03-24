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
                    position: {
                        properties: {
                            "location": {
                                "type": "geo_point"
                            }
                        }
                    },
                    desc: { "type": "text" },
                    zip: { "type": "text" },
                    title: { "type": "text" },
                    category: { "type": "text" },
                    timeStamp: { "type": "date" },
                    twp: { "type": "text" },
                    addr: { "type": "text" }
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
                'position': { location: { 'lat': +(data.lat), 'lon': +(data.lng) } },
                'desc': data.desc,
                'zip': data.zip,
                'category': data.title.split(":")[0],
                'type': data.title.split(": ")[1],
                'timeStamp': new Date(data.timeStamp),
                'twp': data.twp,
                'addr': data.addr
            };
            calls.push(call)
        })
        .on('end', async() => {
            console.log(calls[0])
            const body = calls.reduce((calls, call) => {
                calls.push({ index: { _index: '911_calls', _type: '_doc' } })
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