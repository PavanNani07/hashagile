const { Client } = require('@elastic/elasticsearch');
const client = new Client({ node: 'http://localhost:9200' });
const fs = require('fs');
const csv = require('csv-parser');

async function createCollection(collectionName) {
    try {
        const response = await client.indices.create({ index: collectionName });
        console.log(`Created index: ${collectionName}`, response);
    } catch (error) {
        if (error.meta.body.error.type === 'resource_already_exists_exception') {
            console.log(`Index ${collectionName} already exists`);
        } else {
            console.error(error);
        }
    }
}

async function indexData(collectionName, excludeColumn) {
    const results = [];
    fs.createReadStream('employee.csv')
      .pipe(csv())
      .on('data', (data) => {
          delete data[excludeColumn]; 
          results.push(data);
      })
      .on('end', async () => {
          for (const record of results) {
              await client.index({
                  index: collectionName,
                  body: record
              });
          }
          console.log(`Indexed data in collection: ${collectionName}, excluding column: ${excludeColumn}`);
      });
}

async function searchByColumn(collectionName, columnName, columnValue) {
    const result = await client.search({
        index: collectionName,
        body: {
            query: {
                match: { [columnName]: columnValue }
            }
        }
    });
    console.log(result.hits.hits);
}

async function getEmpCount(collectionName) {
    const result = await client.count({ index: collectionName });
    console.log(`Employee count in collection ${collectionName}: ${result.count}`);
}

async function delEmpById(collectionName, employeeId) {
    const result = await client.deleteByQuery({
        index: collectionName,
        body: {
            query: {
                match: { "Employee ID": employeeId }
            }
        }
    });
    console.log(`Deleted employee with ID ${employeeId} from collection ${collectionName}`);
}

async function getDepFacet(collectionName) {
    const result = await client.search({
        index: collectionName,
        body: {
            aggs: {
                departments: {
                    terms: { field: "Department.keyword" }
                }
            }
        }
    });
    console.log(`Department facet for collection ${collectionName}:`, result.aggregations.departments.buckets);
}


const v_nameCollection = 'hash_miles';
const v_phoneCollection = 'hash_2374';

async function run() {
    await createCollection(v_nameCollection);
    await createCollection(v_phoneCollection);
    await getEmpCount(v_nameCollection);
    await indexData(v_nameCollection, 'Department');
    await indexData(v_phoneCollection, 'Gender');
    await delEmpById(v_nameCollection, 'E02003');
    await getEmpCount(v_nameCollection);
    await searchByColumn(v_nameCollection, 'Department', 'IT');
    await searchByColumn(v_nameCollection, 'Gender', 'Male');
    await searchByColumn(v_phoneCollection, 'Department', 'IT');
    await getDepFacet(v_nameCollection);
    await getDepFacet(v_phoneCollection);
}

run().catch(console.error);
