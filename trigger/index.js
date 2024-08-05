import { handleVoicemailsStream, handleResellersStream, handleCallLogsStream, handleClientsStream, handleTextLogsStream } from './customerioHandlers/handlers.mjs';

export const handler = async (event) => {
    console.log('Function started', JSON.stringify(event));
    try {
        if (event.Records && Array.isArray(event.Records)) {
            console.log('Processing stream events');
            for (const record of event.Records) {
                const tableName = getTableNameFromARN(record.eventSourceARN);
                console.log(`Processing event for table: ${tableName}`);
                
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
                    default:
                        console.log(`Unhandled table: ${tableName}`);
                }
            }
        } else {
            console.log('No stream events to process. This might be a test invocation.');
        }

        console.log('Function completed successfully');
        return {
            statusCode: 200,
            body: JSON.stringify(`Processed ${(event.Records || []).length} stream events.`)
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify('Error processing request: ' + error.message)
        };
    }
};

function getTableNameFromARN(arn) {
    const parts = arn.split('/');
    return parts[1];
}