#!/bin/bash

# Navigate to narwhals directory and store current path
cd narwhals
old_commit=$(git rev-parse --short HEAD)

# Pull latest changes
git pull origin main

# Get new commit hash
new_commit=$(git rev-parse --short HEAD)

# Go back to parent directory
cd ..

# If there were changes, create a commit
if [ "$old_commit" != "$new_commit" ]; then
    git add narwhals
    git commit -m "update submodule from ${old_commit} to ${new_commit}"
    echo "Created commit updating submodule from ${old_commit} to ${new_commit}"
else
    echo "No changes in submodule"
fi
