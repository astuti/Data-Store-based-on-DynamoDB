# Key-value store based on DynamoDB.
[DynamoDB paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

## Vector Clocks
The implementation of a vector clock is in the file `Dynamo_VectorClock.go`.

## Dynamo Nodes
The implementation of a Dynamo node is in the file `Dynamo_Server.go`. This file defines an RPC interface for a Dynamo node. 

## Dynamo Client
The RPC client is in the file `Dynamo_Client.go`. 

## Setup
Setup the runtime environment variables to build the code and also use the executables that will be generated.
1. For Mac, open `~/.bash_profile` or for a unix/linux machine, open `~/.bashrc`. Then add the following:
```
export GOPATH=<path to the code>
export PATH=$PATH:$GOPATH/bin
```
2. Run `source ~/.bash_profile` or `source ~/.bashrc`

## Usage
### Building the code
To build the code, run 
```
./build.sh
```
This should generate `bin/DynamoCoordinator` and `bin/DynamoClient`

### Running the code
To start up a set of nodes, run
```
./run-server.sh [config file]
```
where `config file` is a .ini file. Use `myconfig.ini` in this case.
To run your server in the background, use
```
nohup ./run-server.sh [config file] &
```
This will start the server in the background and append the output to a file called `nohup.out`

To run the client, run
```
./run-client.sh
```

### Unit Testing
To test the code, navigate to `src/mydynamotest/` and run
```
go test
```

To run a specific test in the code, run

```
go test -run [testname]
```