#!/bin/bash
# MongoDB initialization script

# Wait for MongoDB to be available
echo "Waiting for MongoDB to start..."
until nc -z host.docker.internal 27017; do
  echo "MongoDB not available yet - sleeping"
  sleep 2
done

echo "MongoDB is up - initializing connections"

# Test connection to MongoDB
echo "Testing connection to MongoDB..."
mongo --host host.docker.internal:27017 --eval "db.serverStatus()" || {
  echo "Error: Could not connect to MongoDB"
  exit 1
}

# Create or verify the database and collections exist
mongo --host host.docker.internal:27017 <<EOF
  use bigdatatugas;
  if (db.getCollection("2024").count() == 0) {
    print("Collection '2024' is empty. Please load data before running ETL.");
  } else {
    print("Found data in collection '2024'. Ready for ETL process.");
  }
  
  // Ensure the transformed collection exists
  db.createCollection("2024_transformed");
  print("Initialized '2024_transformed' collection for output.");
EOF

echo "MongoDB initialization completed"
