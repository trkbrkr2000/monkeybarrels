import { createReadStream, createWriteStream, read } from 'fs';
import mongoose, { type Model } from 'mongoose';
import { parse } from 'csv-parse';
import { User } from '../models/user.model';
import { z } from 'zod';

export const validateCsv = async <M, P>(options: {
    filePath: string;
    model: Model<M>;
    schema: z.ZodSchema<P>,
    batchSize?: number,
}): Promise<{ values: M[], errors: any[], rowCount: number }> => {
    const values: any = [];
    const errors: any = [];
    const databaseErrors: any = [];

    const { filePath, model, schema, batchSize = 1000 } = options;
    
    return new Promise((resolve, reject) => {
        let rowNumber = 1;
        const readStream = createReadStream(filePath);
        const csvParser = parse({
            delimiter: ',',
            columns: true,
            trim: true,
            skip_empty_lines: true
        });

        readStream.on('error', (err) => {
            console.error(err.message);
            reject(err);
        });

        csvParser.on('error', (err) => {
            console.error(err.message);
            reject(err);
        });

        csvParser.on('data', async (row) => {
            const result = schema.safeParse(row);
            if (result.success) {
                values.push(result.data); // Store the validated data
            } else {
                row.rowNumber = rowNumber;
                errors.push({
                    row,
                    errors: result.error.errors
                }); // Store both the row and the validation errors
            }

            if(values.length >= batchSize) {
                csvParser.pause();
                await model.insertMany(values)
                    .then((docs) => {
                        values.length = 0;
                        csvParser.resume();
                    })
                    .catch((err) => {
                        databaseErrors.push(err);
                        csvParser.resume();
                    });
            }

            rowNumber++;
        });

        csvParser.on('end', async () => {
            if(values.length > 0) {
                await model.insertMany(values)
            }

            resolve({ values, errors, rowCount: rowNumber - 1 });
        });

        // Pipe the read stream to the parser
        readStream.pipe(csvParser);
    });
};


// (async () => {
//     await mongoose.connect('mongodb://localhost:27017/StreamingDB', { bufferCommands: false });

//     const t = Date.now();
//     const { values, errors, rowCount } = await validateCsv<typeof User, Person>({
//         filePath: '../../random_data_large.csv',
//         model: User,
//         schema: personSchema,
//         batchSize: 10000
//     });

//     console.log(errors, rowCount, ((Date.now() - t) / 1000) / 60);

//     await mongoose.disconnect();
// }
// )();