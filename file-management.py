"""
└── File Management Layer
    │
    ├── File Indexing
    │   ├── Scanning shared directories for files
    │   │    └── There should be a settings option, where you can add file paths and all of its subsidiaries.
    │   ├── Storing file metadata (e.g., name, size, hash)
    │   ├── Creating an index of shared files
    │   │    └── Might have to learn hash tables :/
    │   ├── Updating the index when files are added or removed
    │   │    └── Check periodically? Maybe there is something inside the OS library which efficiently checks for
    │   │        changes? I'll have to research
    │   └── Handling indexing errors
    │        └── Unsure about what this means
    │
    ├── Search
    │   ├── Allowing users to search for shared files
    │   ├── Filtering search results based on user input
    │   └── Displaying search results to the user
    │
    ├── Downloading
    │   ├── Allowing users to download shared files
    │   ├── Verifying the file hash to ensure integrity
    │   ├── Showing the download progress
    │   └── Handling download errors
    │
    ├── Uploading
    │   ├── Allowing users to share files
    │   ├── Adding files to the shared directory
    │   ├── Updating the file index
    │   └── Sending file information to connected peers
    │
    └── Shared Files
        ├── Displaying a list of shared files to the user
        ├── Showing file information (e.g., name, size, hash)
        ├── Allowing the user to remove shared files
        └── Handling file removal errors
"""

