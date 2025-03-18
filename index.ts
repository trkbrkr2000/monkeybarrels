
import mongoose from "mongoose";
import path from "path";
import { User } from "./src/models/user.model";
import { validateCsv } from './src/service/transform.service';
import express, { type Request, type Response } from 'express';
import { z } from 'zod';

// Create Express app
const app = express();
const port = 3000;

// Middleware to parse JSON bodies
app.use(express.json());


// Define the schema for a person with the specified fields
export const personSchema = z.object({
    firstname: z.string()
        .min(1, "First name is required")
        .max(100, "First name cannot exceed 100 characters"),

    lastname: z.string()
        .min(1, "Last name is required")
        .max(100, "Last name cannot exceed 100 characters"),

    birthday: z.string()
        .refine(
            (date) => /^\d{4}-\d{2}-\d{2}$/.test(date) && !isNaN(Date.parse(date)),
            { message: "Birthday must be in YYYY-MM-DD format and be a valid date" }
        ),

    favorite_pet: z.string()
        .min(1, "Favorite pet is required")
});

export type Person = z.infer<typeof personSchema>;


// Example of how to use the function
async function example() {
    // await mongoose.connect('mongodb://localhost:27017/StreamingDB', { bufferCommands: false} );
    console.log('Running example');

    const stats = await validateCsv({
        filePath: path.join(__dirname, 'random_data_large.csv'),
        model: User,
        batchSize: 10000,
        schema: personSchema
    });
}

app.get('/', async (req: Request, res: Response) => {
    await example();
    console.log('added rows');
    res.send('Example run');
});

app.get('/users', async (req: Request, res: Response) => {
    const users = await User.find({
    }).limit(1000);
    res.json(users);
});

const start = async () => {
    
    try {
        await mongoose.connect('mongodb://localhost:27017/StreamingDB', { bufferCommands: false });
        console.log('Server connected to MongoDb!');
    } catch (err) {
        console.error(err);
        throw new Error('Database connection error');
    }

    const PORT = 3000;
    app.listen(PORT, () => {
        console.log(`Server is listening on ${PORT}!!!!!!!!!`);
    });
};

start();