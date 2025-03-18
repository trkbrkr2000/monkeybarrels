const fs = require('fs');
const { faker } = require('@faker-js/faker');

// Create a writable stream
const writeStream = fs.createWriteStream('random_data_large.csv');

// Write the header
writeStream.write('firstname,lastname,birthday,favorite_pet\n');

// Common pets for more realistic data
const pets = [
  'Dog', 'Cat', 'Fish', 'Bird', 'Hamster', 'Rabbit', 'Guinea Pig', 
  'Turtle', 'Snake', 'Lizard', 'Horse', 'Parrot', 'Ferret'
];

// Generate a function for creating random rows
function generateRow() {
  const firstname = faker.person.firstName().replace(/,/g, '');
  const lastname = faker.person.lastName().replace(/,/g, '');
  
  // Generate birthday in YYYY-MM-DD format (between 1950 and 2005)
  const birthday = faker.date.between({ 
    from: '1950-01-01', 
    to: '2005-12-31' 
  }).toISOString().split('T')[0];
  
  const favorite_pet = pets[Math.floor(Math.random() * pets.length)];
  
  return `${firstname},${lastname},${birthday},${favorite_pet}\n`;
}

// Number of rows to generate
const totalRows = 1000000;
const batchSize = 10000;
let rowsGenerated = 0;

console.log('Starting CSV generation...');

// Write data in batches to avoid memory issues
function writeBatch() {
  let canContinue = true;
  
  for (let i = 0; i < batchSize && rowsGenerated < totalRows; i++) {
    canContinue = writeStream.write(generateRow());
    rowsGenerated++;
    
    if (rowsGenerated % 100000 === 0) {
      console.log(`Generated ${rowsGenerated.toLocaleString()} rows...`);
    }
  }
  
  if (rowsGenerated < totalRows) {
    // If buffer is full, wait for drain event before continuing
    if (!canContinue) {
      writeStream.once('drain', writeBatch);
    } else {
      // Use setImmediate to avoid blocking the event loop
      setImmediate(writeBatch);
    }
  } else {
    // All rows generated, end the stream
    writeStream.end();
    console.log(`Completed generating ${totalRows.toLocaleString()} rows of data!`);
  }
}

// Start the process
writeBatch();

// Handle finish event
writeStream.on('finish', () => {
  console.log('CSV file has been successfully created!');
  console.log(`File: random_data_large.csv`);
});

// Handle error
writeStream.on('error', (err: any) => {
  console.error('Error writing to file:', err);
});