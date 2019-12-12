# kdb3
database written in Go with sqlite3 as storage and query/view engine. Not ready yet.

Features
  1. Document Database - Done
  2. Optimistic Concurrency - Done
  3. Change tracking - Done
  4. Incrementally updated Materialistic View - Done
  5. Incremental Backup - InProgress
  6. External Replication - InProgress
  7. External Views
  8. Cluster
  9. High Availability with replica


How to Build?

#cgo support required.

go build -tags "json1"

[![asciicast](https://asciinema.org/a/GwSJcYRffxpTph59CLeTKYkmX.svg)](https://asciinema.org/a/GwSJcYRffxpTph59CLeTKYkmX)

<script id="asciicast-GwSJcYRffxpTph59CLeTKYkmX" src="https://asciinema.org/a/GwSJcYRffxpTph59CLeTKYkmX.js" async></script>
