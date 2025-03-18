import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { createWriteStream } from 'fs';
import mongoose from 'mongoose';
import { User } from '../models/user.model';
import { validateCsv, type Person } from './transform.service';
import { personSchema } from './transform.service';

const testFilePath = 'test.csv';

beforeAll(async () => {
    await mongoose.connect('mongodb://localhost:27017/StreamingDBTest', { bufferCommands: false });

    // Create a test CSV file
    const csvData = `firstname,lastname,birthday,favorite_pet
John,Doe,1990-01-01,Dog
Jane,Smith,1985-05-15,Cat
Invalid,User,not-a-date,Parrot
`;

    const writeStream = createWriteStream(testFilePath);
    writeStream.write(csvData);
    writeStream.end();
});

afterAll(async () => {
    if (mongoose.connection.db) {
        await mongoose.connection.db.dropDatabase();
    }
    await mongoose.disconnect();
});

describe('validateCsv', () => {
    it('should validate and insert valid rows, and return errors for invalid rows', async () => {
        const { values, errors, rowCount } = await validateCsv<typeof User, Person>({
            filePath: testFilePath,
            model: User,
            schema: personSchema,
            batchSize: 2
        });

        expect(values.length).toBe(2);
        expect(errors.length).toBe(1);
        expect(rowCount).toBe(3);

        const users = await User.find({});
        expect(users.length).toBe(2);
        expect(users[0].firstname).toBe('John');
        expect(users[1].firstname).toBe('Jane');
    });
});