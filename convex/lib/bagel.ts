// @ts-nocheck
// Define the maximum batch size for upserts
export const MaxUpsertBatchLimit = 100;

// Function to check if BagelDB is available
export async function bagelDBAvailable(): boolean {
    const settings = new Settings({
        bagel_api_impl: "rest",
        bagel_server_host: "api.bageldb.ai",
        bagel_server_http_port: 80,
    });
    const client = new Client(settings);

    return (
        await client.ping() === "pong"
    );
}

// Function to create a BagelDB client
export async function bagelDBClient() {
    const settings = new Settings({
        bagel_api_impl: "rest",
        bagel_server_host: "api.bageldb.ai",
        bagel_server_http_port: 80,
    });

    const client = new Client(settings);
    return client;
}


// Function to create a BagelDB collection
export async function bagelDBIndex() {
    // Settings config
    const settings = new Settings({
        bagel_api_impl: "rest",
        bagel_server_host: "api.bageldb.ai",
        bagel_server_http_port: 80,
    });

    const client = new Client(settings);
    await client.ping();
    const cluster = await client.create_cluster("convex");

    return cluster;
};


// Function to delete vectors in BagelDB
export const deleteVectors = async (clusterName: string, ids: any) => {
    if (!bagelDBAvailable()) {
        throw new Error('Cannot connect to BagelDB. Please check your API key, environment, or cluster name.');
    }

    const client = await bagelDBClient();

    const cluster = await client.get_cluster(clusterName);

    await cluster.delete(ids);
};

// Function to delete all vectors in BagelDB
export const deleteAllVectors = async (clusterName: string) => {
    if (!bagelDBAvailable()) {
        throw new Error('Missing BagelDB API key, environment, or cluster name in environment variables.');
    }

    const client = await bagelDBClient();

    await client.delete_cluster(clusterName);
};


// Function to upsert vectors in BagelDB
export async function upsertVectors(clusterName, ids, vectors) {
    const start = Date.now();
    if (!bagelDBAvailable()) {
        throw new Error('Missing BagelDB API key, environment, or cluster name in environment variables.');
    }

    const client = await bagelDBClient();

    const cluster = await client.get_or_create_cluster(clusterName);

    const results = [];

    // Insert all the vectors in batches
    for (let i = 0; i < vectors.length; i += MaxUpsertBatchLimit) {
        const batch = vectors.slice(i, i + MaxUpsertBatchLimit);
        results.push(
            await cluster.upsert(ids, batch)
        );
    }

    // Log the time taken for upsert
    console.debug(`BagelDB upserted ${vectors.length} vectors in ${Date.now() - start}ms`);
    return results;
}

// Function to query vectors in BagelDB
export async function queryVectors(tableName, embedding, filter, limit) {
    const start = Date.now();
    if (!bagelDBAvailable()) {
        throw new Error('Missing BagelDB API key, environment, or cluster name in environment variables.');
    }

    const client = await bagelDBClient();

    const cluster = await client.get_or_create_cluster(tableName);


    const matches = await cluster.find(embedding, limit, filter);

    if (!matches) {
        throw new Error('No matches found.');
    }



    // Log the time taken for the query
    console.debug(`BagelDB queried ${matches?.length} vectors in ${Date.now() - start}ms`);

    return matches.map((match) => {
        match._id = match.id;
    });
}