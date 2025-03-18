
import mongoose from "mongoose";
import path from "path";
import User from "./src/models/user.model";
import { csvToMongoose } from './src/service/transform.service2';
import express, { type Request, type Response } from 'express';

// Create Express app
const app = express();
const port = 3000;

// Middleware to parse JSON bodies
app.use(express.json());


// Example of how to use the function
async function example() {
    // await mongoose.connect('mongodb://localhost:27017/StreamingDB', { bufferCommands: false} );
    console.log('Running example');

    const stats = await csvToMongoose({
        path: path.join(__dirname, 'random_data_large.csv'),
        model: User,
        batchSize: 10000
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