# SmartDrive
Welcome to the SmartDrive subnet, a pioneering initiative designed to provide secure and distributed information storage solutions. Our mission is to harness the power of Commune's decentralized incentive markets to deliver robust, reliable, and highly secure storage services at scale.

The subnet operates similarly to a blockchain, with validators serving as the nodes responsible for writing information into blocks, maintaining data redundancy, and ensuring synchronization across the network. Additionally, validators are tasked with rating miners based on their performance. Miners are responsible for the actual storage of information and its final retrieval, ensuring data is available when needed.

## ⚠️ Warning ⚠️
SmartDrive is currently in an early stage where runtime errors may occur. However, the information is safe to store, as only the user who uploads it has the ability to access it. At the moment, data duplication is something our team is working on. Currently, a file is only distributed to a single miner, which means the probability of losing the information in this initial phase of the subnet is high.
## Installation
### Manually, on Ubuntu 22.04
- Install Python 3
  ```sh
  sudo apt install python3
- Install Poetry
  ```sh
  pip install poetry
- Enter the Python environment
  ```sh
  poetry shell
- Install the Python dependencies
  ```sh 
  poetry install

## Running a Validator
Validators play a crucial role in maintaining the integrity and security of the SmartDrive storage network. Their responsibilities include:

- Distributing Information: Ensuring data is distributed evenly and securely among miners to prevent single points of failure. Validators manage the distribution process to maintain redundancy and reliability.
- Validating Data: Checking the integrity and availability of stored data by validating sub-chunks across miners. This ensures that the data remains consistent and uncorrupted throughout the network.
- Ensuring Availability: Continuously monitoring the network to ensure that data is always accessible and retrievable. Validators perform regular checks to guarantee that all stored data is available for retrieval when needed.
- Incentivizing Miners: Providing rewards to miners based on their performance in storing and maintaining data integrity. Validators assess miners' contributions and distribute rewards accordingly, encouraging high standards of reliability and performance.

Validators need to run continuously to monitor and validate data, ensuring the network remains robust and secure. Their ongoing efforts are critical in maintaining a high standard of data integrity and availability across the SmartDrive subnet.

### Hardware Requirements
#### Minimum Requirements
- CPU: Dual-core 2.0 GHz
- RAM: 4 GB
- Storage: 100 GB SSD
- Network: High-speed internet connection

#### Recommended Requirements
- CPU: Quad-core 3.0 GHz
- RAM: 8 GB
- Storage: 100 GB SSD
- Network: High-speed internet connection

### Launching a Validator
1. Register the validator on the SmartDrive subnet
````
comx module register <your_validator_name> <your_commune_key> --ip <your_ip_address> --port <port> --netuid <SmartDrive_netuid>  
````

2. Launch the validator
````
python3 -m smartdrive.validator.validator --key <your_commune_key>
````

3. Open TCP port: In order for the validators to connect to each other, it is necessary to open TCP port 9001

Other useful parameters:
- --database_path: Path to the database.
- --port: Default remote api port (Defaults 8001).
- --testnet: Use testnet or not.

Note: There is no need to specify your IP address as the system will automatically obtain the public IP of the device on which the validator is running.


## Running a Miner
The miner is the muscle of the SmartDrive subnet, playing a crucial role in securely and distributed storing user information. As an essential component of the system, miners ensure that data remains accessible and protected against loss or corruption. Thanks to the miners, the network can offer a robust decentralized storage solution, where data is efficiently distributed across multiple nodes. In addition to storing data, miners are also responsible for maintaining the integrity of the information, quickly responding to requests for data retrieval and removal. Their performance is continuously evaluated and rewarded, incentivizing a high level of reliability and efficiency in data storage and management. In summary, miners provide the physical and operational infrastructure that enables the SmartDrive subnet to operate with security, efficiency, and resilience.

### Hardware Requirements
There is not a strict requirements in order to run a miner. However, as a miner, you will be rewarded in base on your service processing requests and storing the data.

### Launching a Miner
1. Register the miner on the SmartDrive subnet
````
comx module register <your_miner_name> <your_commune_key> --ip <your-ip-address> --port <port> --netuid <SmartDrive netuid>  
````

2. Launch the miner
````
python3 -m smartdrive.miner.miner --key <your_commune_key> --name <your_miner_name>
````
Other useful parameters:
- --data_path: Path to the data.
- --max_size: Size (in GB) of path to fill.
- --port: Default remote api port (Defaults 8000).
- --testnet: Use testnet or not.

Note: There is no need to specify your IP address as the system will automatically obtain the public IP of the device on which the validator is running.

## Note
- Make sure to serve and register the miner or the validator using the same key.
- If you are not sure about your public ip address:
```
curl -4 https://ipinfo.io/ip
```
You can check the current subnet uid running:
```
comx subnet list
```
And look for the name SmartDrive

## Running the Subnet Client
The SmartDrive subnet client allows users to interact with the SmartDrive network using three main commands: **store**, **retrieve**, and **remove**. Below are the instructions on how to use each command.

### Installing the cli

```
pip install -e .
```

### Commands
#### Store Command
The store command allows users to initiate the process of storing data in the subnet. Upon initiating this process, a unique identifier (UUID) is generated and returned. This UUID must be saved by the user for future retrieval or removal of the data. The actual storage of the data will be completed subsequently.
```
smartdrive store <file_path> --key-name <your_commune_key>
```

**IMPORTANT**: After executing the store command, save the returned UUID. You will need this UUID to retrieve or remove the stored data later.

#### Retrieve Command
The retrieve command allows users to fetch previously stored data using the unique identifier (UUID) provided at the time of storing.

Usage:
```
smartdrive retrieve <UUID> <output_path> --key-name <your_commune_key>
```

#### Remove Command
The remove command allows users to delete previously stored data from the subnet using the unique identifier (UUID).

Usage:
```
smartdrive remove <UUID> --key-name <your_commune_key>
```

## Roadmap
### 🚀 Initial launch
We launched the subnet in an initial launch to gauge the community's opinion and support. In this initial launch, the foundation of our subnet will begin to take shape, showcasing its potential and possibilities to the entire community.

### First phase
In the first phase, we will focus on improving the security of information verification between validators and miners. To achieve this, we will implement Zero-Knowledge Proofs, which will ensure that a miner possesses a file without the validator needing to know anything about it.

### Second phase
As part of the second phase, we will focus on optimizing the transmission of information and its duplication. This way, the way the subnet handles information will be much faster and more secure.

### ✨ More incoming!
Stay tuned!.
