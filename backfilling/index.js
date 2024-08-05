import { handleResellersStream, handleClientsStream } from './customerioHandlers/handlers.mjs';
import AWS from 'aws-sdk';

const dynamodb = new AWS.DynamoDB.DocumentClient({
    convertEmptyValues: true,
    consistentRead: true
});

export const handler = async (event) => {
    const tables = ['clients', 'resellers'];
    const results = {};
    
    for (const tableName of tables) {
        console.log(`Processing table: ${tableName}`);
        results[tableName] = await processTable(tableName);
    }
    
    console.log('Processing results:', JSON.stringify(results, null, 2));
    return { statusCode: 200, body: 'Backfill completed successfully', results };
};

async function processTable(tableName) {
    const params = { 
        TableName: tableName,
        ConsistentRead: true
    };
    let items = [];
    let data;
    let totalItems = 0;
    let processedItems = 0;
    let failedItems = 0;

    do {
        data = await dynamodb.scan(params).promise();
        items = items.concat(data.Items);
        totalItems += data.Items.length;
        
        const batchResults = await processBatch(tableName, items);
        processedItems += batchResults.processed;
        failedItems += batchResults.failed;
        
        items = [];
        
        params.ExclusiveStartKey = data.LastEvaluatedKey;
    } while (data.LastEvaluatedKey);

    return { totalItems, processedItems, failedItems };
}

async function processBatch(tableName, items) {
    let processed = 0;
    let failed = 0;

    for (const item of items) {
        try {
            const encodedItem = encodeURIComponent(JSON.stringify(item));
            
            const record = {
                eventName: 'MODIFY',
                dynamodb: {
                    NewImage: AWS.DynamoDB.Converter.marshall(JSON.parse(decodeURIComponent(encodedItem)))
                }
            };

            switch (tableName) {
                case 'voicemails':
                    await handleVoicemailsStream(record);
                    break;
                case 'resellers': 
                    await handleResellersStream(record);
                    break;
                case 'textLogs':
                    await handleTextLogsStream(record);
                    break;
                case 'clients':
                    await handleClientsStream(record);
                    break;
                case 'callLogs':
                    await handleCallLogsStream(record);
                    break;
            }
            processed++;
        } catch (error) {
            console.error(`Error processing item in table ${tableName}:`, error);
            console.error('Problematic item:', JSON.stringify(item, null, 2));
            failed++;
        }
    }

    return { processed: processed, failed: failed }; // Return an object with processed and failed counts
}
