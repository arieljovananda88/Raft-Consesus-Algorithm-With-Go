# Raft Consesus Algorithm With Go
Code was made in a private repo that i had no permissions to change visibility, and decided to move it to a personal public repo.

How to build server
1. git clone this repository
2. change directory to src
3. run `` go build ``

How to build client
1. git clone this repository
2. change directory to client
3. run `` go build ``

How to run server
1. run this command on the CLI ``./src --node {node id} --http :{http port for client} --cluster "{node id},:{port for communication between servers}" --leader "{leader node id},:{leader port}"``
   <br />example: ``./src --node 5 --http :2024 --cluster "5,:3034" --leader "2,:3031"``
2. Leader flag is optional, if there is no leader flag node will run as leader from the start

How to run client
1. run this command on the CLI ``./client --port {any port in the cluster}``example: ``./client --port 2020"`

