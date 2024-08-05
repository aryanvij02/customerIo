import Customerio from 'node-customer.io';
import AWS from 'aws-sdk';

const cio = new Customerio('cd5e6728c57c525fa1de', 'c0defee79d2c3b33ebc6');


export async function handleResellersStream(record) {
    // console.log(`Resellers event: ${record.eventName}`);
    // console.log('All row data:', JSON.stringify(record.dynamodb.NewImage || record.dynamodb.OldImage));
    // console.log("This is all records", record)
    
    const allInfo = record.dynamodb.NewImage;
    const userId = encodeURIComponent(allInfo['email_address']?.S || "missingId");
    const emailAddress = allInfo['email_address']?.S || "empty@gmail.com";
    let createdAt;

    if (allInfo['timestamp'] && allInfo['timestamp']['S']) {
        const isoDateTime = allInfo['timestamp']['S'];
        const date = new Date(isoDateTime);
        createdAt = Math.floor(date.getTime() / 1000); // Convert to Unix timestamp in seconds
    } else {
        createdAt = Math.floor(Date.now() / 1000); // Current time in seconds
    }

    // console.log('User ID:', userId);
    // console.log('Email:', emailAddress);
    // console.log('Created At:', createdAt);

    const customerData = processCustomerData({
        ...allInfo,
        created_at: createdAt,
        last_updated: Math.floor(Date.now() / 1000),
        type: "reseller",
        backfill: "true"
    });

    // console.log('Customer Data to be sent:', JSON.stringify(customerData, null, 2));

    try {
        const result = await new Promise((resolve, reject) => {
            cio.identify(userId, customerData, emailAddress, function(err, res) {
                if (err) reject(err);
                else resolve(res);
            });
        });

        // console.log('Customer.io API Response:', {
        //     headers: result.headers,
        //     statusCode: result.statusCode
        // });
    } catch (error) {
        console.error('Error calling Customer.io API:', error);
    }

    // Add your logic for resellers table here
}

export async function handleClientsStream(record) {
    // console.log(`Clients event: ${record.eventName}`);
    const allInfo = record.dynamodb.NewImage;
    
    if (!allInfo) {
        const error = new Error('No new image data found in the record');
        console.error(error.message);
        throw error;
    }

    const rawUserId = allInfo['number']?.S || "missingId";
    const userId = extractEmailUsername(rawUserId);    
    const emailAddress = allInfo['email_address']?.S || "empty@gmail.com";
    const clientType = allInfo.reseller_email ? "reseller_subaccount" : "client";

    if (!clientType) {
        const error = new Error('Client type is empty or undefined');
        console.error(error.message);
        throw error;
    }

    let createdAt;

    if (allInfo['created'] && allInfo['created']['S']) {
        const isoDateTime = allInfo['created']['S'];
        const date = new Date(isoDateTime);
        createdAt = Math.floor(date.getTime() / 1000);
    } else {
        createdAt = Math.floor(Date.now() / 1000);
    }

    // console.log('Processing client:', { userId, emailAddress, clientType, createdAt });

    let customerData;
    if (clientType === "reseller_subaccount") {
        console.log("Identified as reseller_subaccount");
        const reseller_email = allInfo.reseller_email.S

        customerData = processCustomerData({
            ...allInfo,
            created_at: createdAt,
            last_updated: Math.floor(Date.now() / 1000),
            type: clientType,
            reseller_email: reseller_email,
            backfill: "true"
        });
        try {
            console.log("Trying to add a reseller_subaccount")
            const result = await new Promise((resolve, reject) => {
                cio.identify(userId, customerData, emailAddress, function(err, res) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                });
            });
            
            if (result.statusCode !== 200) {
                const error = new Error(`Reseller Subaccount added: ${result.statusCode}`);
                error.response = result;
                throw error;
            }
    
            console.log('Reseller Subaccount:', {
                headers: result.headers,
                statusCode: result.statusCode,
                body: result.body
            });
        } catch (error) {
            console.error('Error adding Reseller Subaccount:', error.message);
            console.error('Error details:', error);
            
            let detailedError;
            if (error.response) {
                detailedError = new Error(`Failed to add client to Customer.io. Status: ${error.response.statusCode}, Body: ${error.response.body}`);
            } else {
                detailedError = new Error(`Failed to add client to Customer.io: ${error.message}`);
            }
            
            detailedError.originalError = error;
            detailedError.clientData = { userId, emailAddress, clientType };
            
            console.error('Detailed error:', detailedError);
            throw detailedError;
        }

    } else {
        customerData = processCustomerData({
            ...allInfo,
            created_at: createdAt,
            last_updated: Math.floor(Date.now() / 1000),
            type: clientType,
            backfill: "true"
        });
        try {
            const result = await new Promise((resolve, reject) => {
                cio.identify(userId, customerData, emailAddress, function(err, res) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                });
            });
    
            if (result.statusCode !== 200) {
                const error = new Error(`Customer.io API returned non-200 status code: ${result.statusCode}`);
                error.response = result;
                throw error;
            }
    
            console.log('Customer.io API Response:', {
                headers: result.headers,
                statusCode: result.statusCode,
                body: result.body
            });
        } catch (error) {
            console.error('Error calling Customer.io API:', error.message);
            console.error('Error details:', error);
            
            let detailedError;
            if (error.response) {
                detailedError = new Error(`Failed to add client to Customer.io. Status: ${error.response.statusCode}, Body: ${error.response.body}`);
            } else {
                detailedError = new Error(`Failed to add client to Customer.io: ${error.message}`);
            }
            
            detailedError.originalError = error;
            detailedError.clientData = { userId, emailAddress, clientType };
            
            console.error('Detailed error:', detailedError);
            throw detailedError;
        }
    }
}

// Helper function to truncate string to stay within 999 bytes
function truncateToBytes(str, maxBytes = 999) {
    const encoder = new TextEncoder();
    if (encoder.encode(str).length <= maxBytes) return str;
    
    let truncated = str;
    while (encoder.encode(truncated).length > maxBytes) {
        truncated = truncated.slice(0, -1);
    }
    return truncated;
}

// Helper function to process customer data
function processCustomerData(data) {
    return Object.entries(data).reduce((acc, [key, value]) => {
        if (typeof value === 'string') {
            acc[key] = truncateToBytes(value);
        } else if (typeof value === 'number' || typeof value === 'boolean') {
            acc[key] = value;
        } else if (value && typeof value === 'object') {
            if (value.S !== undefined) {
                acc[key] = truncateToBytes(value.S);
            } else if (value.N !== undefined) {
                acc[key] = Number(value.N);
            } else if (value.BOOL !== undefined) {
                acc[key] = value.BOOL;
            } else {
                // For nested objects like business_information
                acc[key] = truncateToBytes(JSON.stringify(value));
            }
        }
        return acc;
    }, {});
}

function extractEmailUsername(input) {
    // Decode the input if it's URL encoded
    const decodedInput = decodeURIComponent(input);
    
    let result;
    // Check if the input contains '@' and '.'
    if (decodedInput.includes('@')) {
        // If it looks like an email, return everything before the '@'
        result = decodedInput.split('@')[0];
    } else {
        // If it doesn't look like an email, return the input as is
        result = decodedInput;
    }
    
    // Ensure the result is URL-encoded
    return encodeURIComponent(result);
}