const { Client, Settings } = require('bageldb-beta');
import { Id, TableNames } from '../_generated/dataModel';
import { internalAction } from '../_generated/server';


// Define the maximum batch size for upserts
export const MaxUpsertBatchLimit = 100;

function orThrow(env: string | undefined): string {
    if (!env) throw new Error('Missing Environment Variable');
    return env;
}


// Function to check if BagelDB is available
export async function bagelDBAvailable(): Promise<boolean> {
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
export async function bagelDBIndex(collectionName: string) {
    // Settings config
    const settings = new Settings({
        bagel_api_impl: "rest",
        bagel_server_host: "api.bageldb.ai",
        bagel_server_http_port: 80,
    });

    const client = new Client(settings);
    await client.ping();
    const cluster = await client.get_or_create_cluster(collectionName);

    return cluster;
};


// Function to delete vectors in BagelDB
export const deleteVectors = internalAction({
    handler: async (ctx, { tableName, ids }: { tableName: TableNames; ids: Id<TableNames>[] }) => {
        const bagelCollection = await bagelDBIndex(tableName);

        await bagelCollection.delete(ids = ids);
    },
});

// Function to delete all vectors in BagelDB
export const deleteAllVectors = internalAction({
    args: {},
    handler: async (ctx, args) => {
        if (await bagelDBAvailable()) {
            const client = await bagelDBClient();
            const cluster = await client.get_or_create_cluster(args);
            const deletionResult = await client.delete(cluster);
            return deletionResult;
        } else {
            return {};
        }
    },
});


// Function to upsert vectors in BagelDB
export async function upsertVectors<TableName extends TableNames>(
    tableName: TableName,
    vectors: { id: Id<TableName>; values: number[]; metadata: object }[],
) {
    const start = Date.now();
    const index = await bagelDBIndex(tableName);
    const results = [];

    for (let i = 0; i < vectors.length; i += MaxUpsertBatchLimit) {
        results.push(
            await index.upsert(
                vectors.slice(i, i + MaxUpsertBatchLimit),
            ),
        );
    }

    return results;
}

// Function to query vectors in BagelDB
export async function queryVectors<TableName extends TableNames>(
    tableName: TableName,
    embedding: number[],
    filter: object,
    limit: number,
) {
    const start = Date.now();
    const bagel = await bagelDBIndex(tableName);
    const { ids, distance } = await bagel.find(
        embedding,
        limit,
        filter,
    );

    // Map the results to have a similar structure as Pinecone's query results
    const matches = ids.map((id: string, index: number): { _id: Id<TableName>; score: number } => ({
        _id: id as Id<TableName>,
        score: 1 - (distance[index] as number), // Explicitly cast distance to number
    }));

    return matches;
}



